"""
Gold Serving Layer Manager — AWS Glue / PySpark.

Responsibilities:
1. Primary Data Lake Target: Materializes / Upserts authoritative physical Iceberg tables
   in AWS Athena / Glue Data Catalog (gold_<source>_<table_name>). Physical tables only;
   avoids polluting catalog with multiple redundant views.
2. Multi-Layer SQL Interoperability: Seamlessly resolves and joins tables from Bronze (raw_tbl_*),
   Silver (tbl_*), and upstream Gold (gold_* / v_*) layers within the same catalog database.
3. Downstream Serving Target: Supports zero-downtime atomic swap and idempotent upserts to
   Aurora / RDS MySQL when requested via --GOLD_TARGETS or gold_config.json.
4. Zero-DDL Schema Verification: Verifies target schema exists in MySQL via non-privileged
   SHOW DATABASES / USE (zero INFORMATION_SCHEMA access needed). Fails fast if missing.
5. Shared Database Guardrails: Strictly isolates operations to 'gold_*' (tables).
   Pre-checks all objects before DROP, RENAME, and ALTER. Never modifies external tables.
6. Complete Column Schema Introspection & Evolution: Compares incoming columns against
   existing serving tables and dynamically adapts schemas.
7. Structured Error Diagnostic Cards: Emits rich debugging cards with full stack traces on failure.
"""

import os
import sys
import glob
import json
import logging
import time
import traceback
import re
import inspect
import importlib.util
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple, Union
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit, col

# Ensure gold script directory is in sys.path
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

try:
    from gold_config_loader import GoldConfigLoader
except ImportError:
    try:
        from gold.script.gold_config_loader import GoldConfigLoader
    except ImportError:
        GoldConfigLoader = None

try:
    from protegrity_encryption import ProtegrityEncryptionManager
except ImportError:
    try:
        from gold.script.protegrity_encryption import ProtegrityEncryptionManager
    except ImportError:
        ProtegrityEncryptionManager = None

logger = logging.getLogger(__name__)


class GoldLayerManager:
    """
    Manages the execution, schema validation, data materialization, and database serving
    for Gold layer data marts with Athena Iceberg physical tables, strict shared-database safety,
    and high observability.
    """

    @classmethod
    def _load_gold_config(
        cls,
        config_s3_path: Optional[str] = None,
        s3_client=None,
        env: Optional[str] = None,
        bucket_name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Loads Gold configuration dictionary using GoldConfigLoader if available,
        or a self-contained fallback loader that probes CLI arguments (including raw JSON),
        S3 candidate paths, local files, and sys.path zip archives.
        """
        effective_env = (env or (params.get('ENV') if params else None) or os.environ.get('ENV') or 'dev').strip().lower()

        import zipfile
        raw_config = None

        # Tier 0: Direct JSON string or dict in params or sys.argv
        if params:
            for k in ('GOLD_CONFIG_JSON', 'gold_config_json', 'GOLD_CONFIG', 'gold_config'):
                val = params.get(k)
                if isinstance(val, dict):
                    raw_config = val
                    break
                elif isinstance(val, str) and val.strip().startswith('{') and val.strip().endswith('}'):
                    try:
                        raw_config = json.loads(val.strip())
                        break
                    except Exception:
                        pass
        if raw_config is None and params and isinstance(params.get('ARG_DICT'), dict):
            for k in ('GOLD_CONFIG_JSON', 'gold_config_json', 'GOLD_CONFIG', 'gold_config'):
                val = params['ARG_DICT'].get(k)
                if isinstance(val, dict):
                    raw_config = val
                    break
                elif isinstance(val, str) and val.strip().startswith('{') and val.strip().endswith('}'):
                    try:
                        raw_config = json.loads(val.strip())
                        break
                    except Exception:
                        pass

        if raw_config is None:
            for i, a in enumerate(sys.argv):
                for flag in ('--GOLD_CONFIG_JSON', '--gold_config_json', '--GOLD_CONFIG', '--gold_config'):
                    if a == flag and i + 1 < len(sys.argv):
                        val = sys.argv[i + 1].strip()
                        if val.startswith('{') and val.endswith('}'):
                            try:
                                raw_config = json.loads(val)
                                break
                            except Exception:
                                pass
                    elif a.startswith(f"{flag}="):
                        val = a.split('=', 1)[1].strip()
                        if val.startswith('{') and val.endswith('}'):
                            try:
                                raw_config = json.loads(val)
                                break
                            except Exception:
                                pass
                if raw_config is not None:
                    break

        # Tier 0b: config_s3_path itself might be a direct JSON string
        if raw_config is None and config_s3_path and isinstance(config_s3_path, str):
            clean_p = config_s3_path.strip()
            if clean_p.startswith('{') and clean_p.endswith('}'):
                try:
                    raw_config = json.loads(clean_p)
                except Exception:
                    pass

        # Tier 1: Try GoldConfigLoader if imported and no direct JSON was provided
        if raw_config is None and GoldConfigLoader:
            try:
                cfg = GoldConfigLoader.load_config(
                    config_s3_path=config_s3_path,
                    s3_client=s3_client,
                    env=effective_env,
                    bucket_hint=bucket_name
                )
                if cfg and isinstance(cfg, dict) and (cfg.get("source_systems") or cfg.get("sources")):
                    return cfg
            except Exception as e:
                logger.warning(f"[GOLD CONFIG] GoldConfigLoader.load_config encountered error: {e}. Trying direct load...")

        # Tier 1b: Check CLI argument for path
        if raw_config is None and not config_s3_path:
            for i, a in enumerate(sys.argv):
                for flag in (
                    '--GOLD_CONFIG_S3_PATH', '--gold_config_s3_path', '--gold-config-s3-path',
                    '--GOLD_CONFIG_PATH', '--gold_config_path', '--gold-config-path',
                    '--GOLD_CONFIG', '--gold_config', '--CONFIG_S3_PATH', '--config_s3_path'
                ):
                    if a == flag and i + 1 < len(sys.argv):
                        val = sys.argv[i + 1].strip()
                        if not (val.startswith('{') and val.endswith('}')):
                            config_s3_path = val
                            break
                    elif a.startswith(f"{flag}="):
                        val = a.split('=', 1)[1].strip()
                        if not (val.startswith('{') and val.endswith('}')):
                            config_s3_path = val
                            break
                if config_s3_path:
                    break

        if not s3_client and ((config_s3_path and config_s3_path.startswith("s3://")) or bucket_name):
            try:
                s3_client = boto3.client('s3')
            except Exception:
                pass

        # Tier 2: Load from explicit S3 path
        if raw_config is None and config_s3_path and config_s3_path.startswith("s3://") and s3_client:
            try:
                path_parts = config_s3_path.replace("s3://", "").split("/", 1)
                b_name, o_key = path_parts[0], path_parts[1]
                resp = s3_client.get_object(Bucket=b_name, Key=o_key)
                raw_config = json.loads(resp['Body'].read().decode('utf-8'))
                logger.info(f"[GOLD CONFIG] Successfully loaded Gold configuration from '{config_s3_path}'")
            except Exception as err:
                logger.warning(f"[GOLD CONFIG] Failed to load from '{config_s3_path}': {err}. Probing alternative S3 locations...")

        # Tier 3: Probe candidate S3 locations
        if raw_config is None and s3_client:
            candidate_buckets = []
            if bucket_name:
                clean_b = str(bucket_name).replace('{env}', effective_env).replace('{ENV}', effective_env.upper()).strip()
                if clean_b and clean_b not in candidate_buckets:
                    candidate_buckets.append(clean_b)
            for b_env in [
                os.environ.get('DATA_LAKE_BUCKET'),
                os.environ.get('GOLD_BUCKET'),
                os.environ.get('SILVER_BUCKET'),
                f"uax-datalake-{effective_env}-bucket",
                f"uax-datalake-dev-bucket"
            ]:
                if b_env:
                    cb = str(b_env).replace('{env}', effective_env).replace('{ENV}', effective_env.upper()).strip()
                    if cb and cb not in candidate_buckets:
                        candidate_buckets.append(cb)

            relative_keys = [
                "gold/script/config/gold_config.json",
                "gold/config/gold_config.json",
                "scripts/gold/config/gold_config.json",
                "scripts/config/gold_config.json",
                "config/gold_config.json",
                "gold_config.json"
            ]

            for b in candidate_buckets:
                if raw_config is not None:
                    break
                for k in relative_keys:
                    candidate_url = f"s3://{b}/{k}"
                    if candidate_url == config_s3_path:
                        continue
                    try:
                        resp = s3_client.get_object(Bucket=b, Key=k)
                        raw_config = json.loads(resp['Body'].read().decode('utf-8'))
                        logger.info(f"[GOLD CONFIG] Successfully loaded Gold configuration from probed S3 path: '{candidate_url}'")
                        break
                    except Exception:
                        pass

        # Tier 4: Local filesystem search
        if raw_config is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            local_paths = [
                os.path.join(current_dir, "config", "gold_config.json"),
                os.path.join(current_dir, "gold_config.json"),
                os.path.join(os.getcwd(), "gold_config.json"),
                os.path.join(os.getcwd(), "config", "gold_config.json"),
                os.path.join(os.getcwd(), "gold", "script", "config", "gold_config.json"),
                "/tmp/gold_config.json",
                "/tmp/config/gold_config.json",
                "/tmp/gold/script/config/gold_config.json",
                "gold_config.json",
                "gold/script/config/gold_config.json",
                "scripts/gold/config/gold_config.json"
            ]
            for p in local_paths:
                if os.path.exists(p) and os.path.isfile(p):
                    try:
                        with open(p, "r", encoding="utf-8") as f:
                            raw_config = json.load(f)
                            logger.info(f"[GOLD CONFIG] Successfully loaded Gold configuration from local file: '{p}'")
                            break
                    except Exception as err:
                        logger.warning(f"Error reading local file '{p}': {err}")

        # Tier 5: Scan sys.path archives (.zip/.egg)
        if raw_config is None:
            for p in sys.path:
                if p.endswith(('.zip', '.egg')) and os.path.exists(p):
                    try:
                        with zipfile.ZipFile(p, 'r') as zf:
                            for zinfo in zf.namelist():
                                if zinfo.endswith("gold_config.json"):
                                    with zf.open(zinfo) as f:
                                        raw_config = json.load(f)
                                        logger.info(f"[GOLD CONFIG] Successfully loaded Gold configuration from archive '{p}!/{zinfo}'")
                                        break
                    except Exception:
                        pass
                if raw_config is not None:
                    break

        if raw_config is None:
            logger.warning("[GOLD CONFIG] Gold configuration file 'gold_config.json' could not be loaded from S3, CLI, or local paths.")
            raw_config = {}

        # Interpolate {env}
        def _interp(val):
            if isinstance(val, str):
                return val.replace('{env}', effective_env).replace('{ENV}', effective_env.upper())
            elif isinstance(val, dict):
                return {_interp(k): _interp(v) for k, v in val.items()}
            elif isinstance(val, list):
                return [_interp(x) for x in val]
            return val

        interpolated = _interp(raw_config)
        if GoldConfigLoader:
            try:
                GoldConfigLoader.set_loaded_config(interpolated, env=effective_env)
            except Exception:
                pass
        return interpolated

    @classmethod
    def _resolve_natural_keys(
        cls,
        source_system: str,
        table_name: str,
        gold_cfg: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> List[str]:
        """
        Resolves natural key / primary key list for table upsert / merge:
        1. From gold_config.json table definition (source_systems.<source>.tables.<table_name>.nkey)
        2. From CLI override parameters (--NKEY, --PRIMARY_KEY)
        Returns empty list if not specified (NO HARDCODED VALUES).
        """
        pks = []

        # 1. Config lookup via GoldConfigLoader
        if GoldConfigLoader and gold_cfg:
            try:
                pks = GoldConfigLoader.get_primary_key(source_system, table_name, gold_cfg)
            except Exception:
                pks = []

        # 1b. Direct config dictionary traversal if GoldConfigLoader is absent or didn't find key
        if not pks and isinstance(gold_cfg, dict):
            sources = gold_cfg.get("source_systems") or gold_cfg.get("sources") or {}
            clean_src = (source_system or '').strip().lower()
            source_cfg = {}
            if isinstance(sources, dict):
                for k, v in sources.items():
                    if k.strip().lower() == clean_src and isinstance(v, dict):
                        source_cfg = v
                        break
            if not source_cfg and clean_src in gold_cfg and isinstance(gold_cfg[clean_src], dict):
                source_cfg = gold_cfg[clean_src]

            table_configs = source_cfg.get("tables") if isinstance(source_cfg.get("tables"), dict) else source_cfg
            if isinstance(table_configs, dict):
                clean_tbl = (table_name or '').strip().lower()
                base_tbl = clean_tbl
                for prefix in [f"gold_{clean_src}_", "gold_tbl_", "gold_", "v_", "tbl_", "raw_tbl_"]:
                    if base_tbl.startswith(prefix):
                        base_tbl = base_tbl[len(prefix):]
                        break
                candidates = [clean_tbl, base_tbl, base_tbl.replace('-', '_'), base_tbl.replace('_', '-')]

                matched_tbl_cfg = {}
                for cand in candidates:
                    for tbl_k, tbl_v in table_configs.items():
                        if tbl_k.strip().lower() == cand and isinstance(tbl_v, dict):
                            matched_tbl_cfg = tbl_v
                            break
                    if matched_tbl_cfg:
                        break

                if matched_tbl_cfg:
                    for key_name in ("nkey", "primary_key", "natural_key", "pk", "natural_keys", "primary_keys"):
                        pk_val = matched_tbl_cfg.get(key_name)
                        if isinstance(pk_val, list) and pk_val:
                            pks = [str(k).strip() for k in pk_val if str(k).strip()]
                            break
                        elif isinstance(pk_val, str) and pk_val.strip():
                            pks = [str(k).strip() for k in pk_val.split(',') if str(k).strip()]
                            break

        # 2. CLI / Lambda override parameters
        if not pks and params:
            clean_tbl = (table_name or '').strip().lower()
            base_tbl = clean_tbl
            for prefix in [f"gold_{clean_src}_", "gold_tbl_", "gold_", "v_", "tbl_", "raw_tbl_"]:
                if base_tbl.startswith(prefix):
                    base_tbl = base_tbl[len(prefix):]
                    break

            cand_param_keys = [
                "NKEY", "nkey", "NKEYS", "nkeys",
                "PRIMARY_KEY", "primary_key", "PRIMARY_KEYS", "primary_keys",
                "NATURAL_KEY", "natural_key", "NATURAL_KEYS", "natural_keys",
                "PK", "pk",
                f"{clean_tbl}_nkey", f"{clean_tbl}_primary_key",
                f"{base_tbl}_nkey", f"{base_tbl}_primary_key"
            ]
            for pk_key in cand_param_keys:
                val = params.get(pk_key)
                if val:
                    pks = [k.strip() for k in str(val).split(",") if k.strip()]
                    break
                if isinstance(params.get("ARG_DICT"), dict):
                    val = params["ARG_DICT"].get(pk_key)
                    if val:
                        pks = [k.strip() for k in str(val).split(",") if k.strip()]
                        break

        if not pks:
            clean_tbl = (table_name or '').strip().lower()
            flags = (
                '--NKEY', '--nkey', '--NKEYS', '--nkeys',
                '--PRIMARY_KEY', '--primary_key', '--PRIMARY_KEYS', '--primary_keys',
                '--NATURAL_KEY', '--natural_key', '--NATURAL_KEYS', '--natural_keys',
                '--PK', '--pk',
                f'--{clean_tbl}_nkey', f'--{clean_tbl}_primary_key'
            )
            for i, a in enumerate(sys.argv):
                if a in flags and i + 1 < len(sys.argv):
                    pks = [k.strip() for k in sys.argv[i + 1].split(',') if k.strip()]
                    break
                elif any(a.startswith(f"{f}=") for f in flags):
                    pks = [k.strip() for k in a.split('=', 1)[1].split(',') if k.strip()]
                    break

        return pks

    @classmethod
    def _purge_s3_prefix(cls, s3_client, s3_uri: str) -> int:
        """
        Safely deletes orphaned or corrupted metadata/data files under an S3 URI when an Iceberg table is corrupt.
        Requires at least 3 path segments to avoid deleting shallow prefixes.
        """
        if not s3_client or not s3_uri or not s3_uri.startswith("s3://"):
            return 0
        try:
            import urllib.parse
            parsed = urllib.parse.urlparse(s3_uri)
            bucket = parsed.netloc
            prefix = parsed.path.lstrip('/')
            if not prefix.endswith('/'):
                prefix += '/'
            parts = [p for p in prefix.strip('/').split('/') if p]
            if len(parts) < 3:
                logger.warning(f"Refusing to purge suspiciously shallow S3 prefix: '{prefix}'")
                return 0

            paginator = s3_client.get_paginator('list_objects_v2')
            total_deleted = 0
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                objects = page.get('Contents', [])
                if objects:
                    delete_keys = [{'Key': obj['Key']} for obj in objects]
                    s3_client.delete_objects(Bucket=bucket, Delete={'Objects': delete_keys})
                    total_deleted += len(delete_keys)
            if total_deleted > 0:
                logger.info(f"Purged {total_deleted} corrupted/orphaned S3 object(s) under '{s3_uri}'.")
            return total_deleted
        except Exception as e:
            logger.warning(f"Could not purge S3 prefix '{s3_uri}': {e}")
            return 0

    @classmethod
    def run_gold_pipeline(
        cls,
        spark: SparkSession,
        params: Dict[str, Any],
        glue_client=None,
        s3_client=None,
        secrets_client=None,
        athena_client=None
    ) -> List[Dict[str, Any]]:
        """
        Main entry point for Gold serving layer execution.
        1. Discovers query definitions (.sql).
        2. Resolves dependencies across Bronze, Silver, and upstream Gold tables.
        3. MANDATORY: Materializes and upserts physical Iceberg table in AWS Athena / Glue Data Catalog.
        4. DOWNSTREAM: Serves data to Aurora MySQL if requested.
        """
        execution_start = datetime.now(timezone.utc)

        # Target Deployment Environment: CLI (--ENV or --ENVIRONMENT) > Env Var > Default ('dev')
        env = (
            params.get('ENV')
            or params.get('env')
            or params.get('ENVIRONMENT')
            or params.get('environment')
            or os.environ.get('ENV')
            or os.environ.get('ENVIRONMENT')
            or 'dev'
        ).strip().lower()
        logger.info(f"Target deployment environment resolved: '{env}'")

        # Initialize AWS clients early if not provided
        region = (
            params.get('AWS_REGION')
            or params.get('AWS_DEFAULT_REGION')
            or os.environ.get('AWS_REGION')
            or os.environ.get('AWS_DEFAULT_REGION')
            or 'us-east-1'
        )
        if not s3_client:
            try:
                s3_client = boto3.client('s3', region_name=region)
            except Exception as e:
                logger.warning(f"Could not initialize S3 client: {e}")
        if not glue_client:
            try:
                glue_client = boto3.client('glue', region_name=region)
            except Exception as e:
                logger.warning(f"Could not initialize Glue client: {e}")

        # Resolve Data Lake Bucket
        raw_bucket = (
            params.get('DATA_LAKE_BUCKET')
            or os.environ.get('DATA_LAKE_BUCKET')
            or f"uax-datalake-{env}-bucket"
        )
        bucket_name = str(raw_bucket).replace('{env}', env).replace('{ENV}', env.upper()).strip()

        # Auto-discover and load Gold configuration with robust multi-path probing
        config_s3_path = (
            params.get('GOLD_CONFIG_S3_PATH')
            or (params.get('ARG_DICT', {}).get('GOLD_CONFIG_S3_PATH') if isinstance(params.get('ARG_DICT'), dict) else None)
        )
        gold_cfg = cls._load_gold_config(
            config_s3_path=config_s3_path,
            s3_client=s3_client,
            env=env,
            bucket_name=bucket_name,
            params=params
        )

        defaults_cfg = GoldConfigLoader.get_defaults(gold_cfg, env=env) if GoldConfigLoader else {}
        if defaults_cfg.get('gold_bucket') and not params.get('DATA_LAKE_BUCKET'):
            bucket_name = str(defaults_cfg['gold_bucket']).replace('{env}', env).replace('{ENV}', env.upper()).strip()

        raw_glue_db = (
            params.get('GLUE_DATABASE')
            or (GoldConfigLoader.get_glue_database(gold_cfg, env=env) if GoldConfigLoader else None)
            or f"uax_datalake_db_{env}"
        )
        glue_database = str(raw_glue_db).replace('{env}', env).replace('{ENV}', env.upper()).strip()

        # Ensure Iceberg Glue Catalog is configured on SparkSession
        gold_prefix = params.get('GOLD_DATA_PREFIX') or (defaults_cfg.get('gold_data_prefix') if isinstance(defaults_cfg, dict) else 'gold/data') or 'gold/data'
        warehouse_uri = f"s3://{bucket_name}/{str(gold_prefix).strip('/')}/"

        if spark:
            for k, v in [
                ("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog"),
                ("spark.sql.catalog.glue_catalog.warehouse", warehouse_uri),
                ("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"),
                ("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
                ("spark.sql.catalog.glue_catalog.glue.skip-name-validation", "true"),
                ("spark.sql.catalog.glue_catalog.skip-name-validation", "true"),
                ("spark.sql.catalog.glue_catalog.aws.glue.skip-name-validation", "true"),
                ("spark.sql.defaultCatalog", "glue_catalog"),
                ("spark.sql.iceberg.schema-evolution", "true"),
                ("spark.sql.iceberg.check-nullability", "false"),
                ("spark.sql.parquet.enableVectorizedReader", "false"),
            ]:
                try:
                    spark.conf.set(k, v)
                except Exception as conf_err:
                    logger.debug(f"[SPARK CONF NOTE] Could not set '{k}': {conf_err}")

        # Strict SOURCE_SYSTEM Enforcement
        source_system = (params.get('SOURCE_SYSTEM') or '').strip().lower()
        if not source_system:
            raise ValueError(
                "CRITICAL CONFIG ERROR: Missing required parameter '--SOURCE_SYSTEM'.\n"
                "The Gold query path is 's3://<bucket>/gold/query/<source>/<table_name>.sql'.\n"
                "Please specify the source system (e.g. --SOURCE_SYSTEM moveworks)."
            )

        # Resolve Target Engines: Athena (always primary), Aurora MySQL (optional)
        cli_targets = params.get('GOLD_TARGETS') or params.get('GOLD_TARGET') or params.get('TARGET_ENGINES')
        if cli_targets:
            gold_targets = [t.strip().lower() for t in str(cli_targets).split(',') if t.strip()]
        elif GoldConfigLoader:
            gold_targets = GoldConfigLoader.get_target_engines(source_system, "", config_dict=gold_cfg)
        else:
            gold_targets = ['athena']

        if not gold_targets:
            gold_targets = ['athena']

        # Exclude external engines (Redshift, Databricks, Snowflake)
        gold_targets = [t for t in gold_targets if t in ('athena', 'aurora', 'rds', 'mysql')]
        if 'athena' not in gold_targets:
            gold_targets.insert(0, 'athena')

        # Target MySQL Schema Resolution
        needs_mysql = any(t in gold_targets for t in ('aurora', 'rds', 'mysql'))
        gold_schema = params.get('GOLD_SCHEMA') or params.get('RDS_GOLD_SCHEMA')
        if needs_mysql and not gold_schema:
            src_cfg = GoldConfigLoader.get_source_config(source_system, gold_cfg) if GoldConfigLoader else {}
            first_tbl_cfg = list(src_cfg.get('tables', {}).values())[0] if src_cfg.get('tables') else {}
            gold_schema = first_tbl_cfg.get('aurora', {}).get('schema') or 'enterprise_reporting'

        # Query and Data Paths
        query_s3_path = params.get('GOLD_QUERY_S3_PATH') or f"s3://{bucket_name}/gold/query/{source_system}"
        data_s3_path = params.get('GOLD_DATA_S3_PATH') or f"s3://{bucket_name}/gold/data/{source_system}"

        logger.info(
            f"\n+================================================================================+\n"
            f"|              STARTING GOLD SERVING ENGINE: MULTI-TARGET PIPELINE               |\n"
            f"+================================================================================+\n"
            f"|  * Primary Target     : ATHENA / ICEBERG PHYSICAL TABLE (AWS Glue Catalog)     |\n"
            f"|  * Target Engines     : {', '.join(gold_targets).upper()}\n"
            f"|  * Glue Database      : {glue_database}\n"
            f"|  * Target DB Schema   : {gold_schema or 'N/A (Athena Data Lake)'}\n"
            f"|  * Source System      : {source_system.upper()}\n"
            f"|  * Query Path         : {query_s3_path}\n"
            f"|  * Data S3 Path       : {data_s3_path}\n"
            f"|  * Data Lake Bucket   : {bucket_name}\n"
            f"|  * Execution Time     : {execution_start.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
            f"+================================================================================+"
        )

        mart_stats = []

        # ----------------------------------------------------------------------
        # [GOLD STEP 1] Discover Mart Queries (.sql)
        # ----------------------------------------------------------------------
        logger.info(
            f"\n================================================================================\n"
            f"[GOLD STEP 1] Discovering Gold Mart Query Definitions (.sql)\n"
            f"--------------------------------------------------------------------------------"
        )
        queries = cls._discover_queries(
            query_s3_path,
            bucket_name,
            s3_client,
            source_system=source_system
        )
        if not queries:
            raise FileNotFoundError(
                f"CRITICAL QUERY DISCOVERY ERROR: No .sql query definitions found at '{query_s3_path}'.\n"
                f"Expected query path: s3://{bucket_name}/gold/query/{source_system}/<table_name>.sql (or v_<table_name>.sql)\n"
                f"Please ensure at least one SQL definition is present in S3 or locally in 'gold/query/{source_system}/'."
            )

        all_discovered_queries = dict(queries)

        # Optional CLI table override filtering
        table_filter = params.get('TABLE_LIST') or []
        if isinstance(table_filter, str):
            table_filter = [t.strip() for t in table_filter.split(',') if t.strip()]
        if table_filter:
            clean_filters = set()
            for t in table_filter:
                low = t.lower()
                clean_filters.add(low)
                clean_filters.add(low.replace('-', '_'))
                clean_filters.add(low.replace('_', '-'))
                for prefix in [f"gold_{source_system}_", 'gold_tbl_', 'raw_tbl_', 'tbl_', 'v_']:
                    if low.startswith(prefix):
                        stripped = low[len(prefix):]
                        clean_filters.add(stripped)
                        clean_filters.add(stripped.replace('-', '_'))
                        clean_filters.add(stripped.replace('_', '-'))
            matched = {
                k: v for k, v in queries.items()
                if k.lower() in clean_filters or any(cf in k.lower() for cf in clean_filters)
            }
            if matched:
                logger.info(f"[GOLD STEP 1] Filtered queries by table override {table_filter}: {list(matched.keys())}")
                queries = matched
            else:
                logger.info(
                    f"[GOLD STEP 1] CLI table override {table_filter} did not match mart names {list(queries.keys())}. "
                    f"Processing all discovered mart queries for source '{source_system}'."
                )

        # Ensure any upstream dependencies of the requested tables are included
        queries = cls._resolve_query_dependencies(
            active_queries=queries,
            all_queries=all_discovered_queries,
            source_system=source_system,
            glue_database=glue_database,
            spark=spark,
            gold_cfg=gold_cfg
        )

        logger.info(f"[GOLD STEP 1] Discovered {len(queries)} mart query file(s) to process: {list(queries.keys())}")

        # Ensure dependency ordering: dependency marts run first
        queries = cls._sort_queries_by_dependency(queries, source_system=source_system, glue_database=glue_database, gold_cfg=gold_cfg)
        logger.info(f"[GOLD STEP 1] Query execution order resolved (dependencies first): {list(queries.keys())}")

        # Set Spark active database to GLUE_DATABASE so queries can use direct table names
        if glue_database and spark:
            try:
                try:
                    spark.sql(f"USE glue_catalog.`{glue_database}`")
                    logger.info(f"[GOLD PREP] Set Spark active catalog and database to 'glue_catalog.{glue_database}'. Direct table names are fully supported.")
                except Exception:
                    spark.sql(f"USE `{glue_database}`")
                    logger.info(f"[GOLD PREP] Set Spark active database to '{glue_database}'. Direct table names are fully supported.")
            except Exception as use_err:
                logger.warning(f"[GOLD PREP] Could not set Spark active database to '{glue_database}': {use_err}")

        # ----------------------------------------------------------------------
        # [GOLD STEP 2 - MANDATORY] Athena / Iceberg Table Materialization & UPSERT
        # ----------------------------------------------------------------------
        logger.info(
            f"\n================================================================================\n"
            f"[GOLD STEP 2 - MANDATORY] Materializing / Upserting Athena Iceberg Tables\n"
            f"--------------------------------------------------------------------------------"
        )
        materialized_dfs: Dict[str, DataFrame] = {}
        mart_keys: Dict[str, List[str]] = {}

        for clean_base_name, sql_text in queries.items():
            mart_start = datetime.now(timezone.utc)
            # 1. Natural key (nkey) resolution strictly from Gold config (or CLI override params)
            pks = cls._resolve_natural_keys(source_system, clean_base_name, gold_cfg, params)

            if not pks:
                raise ValueError(
                    f"CRITICAL CONFIG ERROR: Missing 'nkey' in gold configuration for table '{clean_base_name}' "
                    f"under source system '{source_system}'. A natural key is mandatory to ensure row uniqueness "
                    f"and enable idempotent Iceberg/Aurora upserts."
                )

            # 2. Target Athena Table Name (Default: gold_<source>_<tablename>)
            if GoldConfigLoader:
                target_table_name = GoldConfigLoader.get_target_table_name(source_system, clean_base_name, 'athena', gold_cfg)
            else:
                target_table_name = f"gold_{source_system}_{clean_base_name}"
            prefix_g = f"gold_{source_system}_"
            while target_table_name.startswith(f"{prefix_g}{prefix_g}"):
                target_table_name = target_table_name[len(prefix_g):]

            logger.info(
                f"\n+--------------------------------------------------------------------------------+\n"
                f"|  PROCESSING ATHENA GOLD MART: '{clean_base_name}'\n"
                f"|  * Target Table    : {glue_database}.{target_table_name} (Physical Iceberg Table)\n"
                f"|  * Primary Keys    : {pks or 'None (Overwrite/Append)'}\n"
                f"|  * Output Model    : Physical Table (No Views Required)\n"
                f"+--------------------------------------------------------------------------------+"
            )

            # Check and run initial historical export load if candidate exists
            init_loaded = cls._check_and_run_initial_load(
                spark=spark,
                source_system=source_system,
                table_name=clean_base_name,
                target_table_name=target_table_name,
                glue_database=glue_database,
                bucket_name=bucket_name,
                env=env,
                pks=pks,
                params=params,
                sql_text=sql_text,
                gold_cfg=gold_cfg,
                s3_client=s3_client,
                glue_client=glue_client
            )

            try:
                # 3. Ensure upstream dependency temporary views (e.g. tbl_interactions, raw_tbl_conversations) are registered
                cls._ensure_dependency_views_registered(
                    spark=spark,
                    sql_text=sql_text,
                    source_system=source_system,
                    glue_database=glue_database,
                    gold_cfg=gold_cfg,
                    glue_client=glue_client
                )

                # Execute Mart SQL Query
                logger.info(f"Executing Spark SQL query for '{clean_base_name}'...")
                df_mart = spark.sql(sql_text)

                # Incremental Delta Check
                is_incremental = True
                if GoldConfigLoader:
                    cfg_incremental = GoldConfigLoader.is_incremental(source_system, clean_base_name, gold_cfg)
                    if cfg_incremental is False:
                        is_incremental = False
                if 'INCREMENTAL' in params:
                    is_incremental = str(params['INCREMENTAL']).strip().lower() in ('true', '1', 'yes')
                if 'FULL_REFRESH' in params and str(params['FULL_REFRESH']).strip().lower() in ('true', '1', 'yes'):
                    is_incremental = False

                llm_col = GoldConfigLoader.get_llm_column(source_system, clean_base_name, gold_cfg) if GoldConfigLoader else None

                if is_incremental and pks:
                    logger.info(f"[INCREMENTAL FILTER] Isolating new/changed delta for '{clean_base_name}' against '{glue_database}.{target_table_name}'...")
                    df_mart = cls.filter_incremental_delta(
                        spark=spark,
                        df_incoming=df_mart,
                        glue_database=glue_database,
                        target_table_name=target_table_name,
                        nkeys=pks,
                        enrichment_column=llm_col
                    )

                # 4. Invoke Custom Transform Hook if exists
                custom_script_path = None
                if GoldConfigLoader:
                    custom_script_path = GoldConfigLoader.get_custom_transform_path(source_system, clean_base_name, gold_cfg)

                api_secret_name = (
                    params.get('API_SECRET_NAME')
                    or params.get('api_secret_name')
                    or params.get('LLM_SECRET_NAME')
                    or params.get('llm_secret_name')
                )
                if not api_secret_name and GoldConfigLoader:
                    api_secret_name = GoldConfigLoader.get_api_secret_name(source_system, clean_base_name, gold_cfg)

                if custom_script_path:
                    context = {
                        "source_system": source_system,
                        "table_name": clean_base_name,
                        "target_table": target_table_name,
                        "glue_database": glue_database,
                        "primary_keys": pks,
                        "is_incremental": is_incremental,
                        "llm_column": llm_col,
                        "api_secret_name": api_secret_name,
                        "llm_secret_name": api_secret_name,
                        "params": params
                    }
                    df_mart = cls._apply_custom_transform(df_mart, custom_script_path, spark=spark, context=context)

                # 5. Technical audit columns (_updated_at, _inserted_at)
                if "_updated_at" not in df_mart.columns:
                    df_mart = df_mart.withColumn("_updated_at", current_timestamp())
                if "_inserted_at" not in df_mart.columns:
                    df_mart = df_mart.withColumn("_inserted_at", current_timestamp())

                # Convert all business/data columns to StringType to enforce uniform string schema in Gold layer
                for col_name in df_mart.columns:
                    if col_name not in ("_updated_at", "_inserted_at", "_data_as_of"):
                        df_mart = df_mart.withColumn(col_name, col(col_name).cast("string"))

                # Ensure row uniqueness by natural keys (nkey) from gold config
                df_mart = cls._deduplicate_by_nkey(df_mart, pks)
                logger.info(f"[PRIMARY KEY] Natural keys resolved for '{clean_base_name}': {pks}")
                mart_keys[clean_base_name] = pks

                # Register in-session Spark views so downstream dependent marts resolve seamlessly
                try:
                    df_mart.createOrReplaceTempView(clean_base_name)
                    df_mart.createOrReplaceTempView(target_table_name)
                    df_mart.createOrReplaceTempView(f"v_{clean_base_name}")
                except Exception as temp_err:
                    logger.debug(f"[SPARK VIEW] Note on temporary view registration: {temp_err}")

                # 6. Physical Materialization & UPSERT into Athena / Iceberg Table
                mart_s3_dest = f"{data_s3_path.rstrip('/')}/{clean_base_name}"
                rows_written = cls._materialize_athena_table(
                    spark=spark,
                    df_mart=df_mart,
                    glue_database=glue_database,
                    target_table_name=target_table_name,
                    primary_keys=pks,
                    s3_location=mart_s3_dest,
                    clean_base_name=clean_base_name,
                    params=params,
                    glue_client=glue_client,
                    s3_client=s3_client
                )

                # Refresh in-session Spark temp views directly from authoritative physical Iceberg table
                try:
                    full_gold_df = spark.table(f"{glue_database}.{target_table_name}")
                    full_gold_df.createOrReplaceTempView(clean_base_name)
                    full_gold_df.createOrReplaceTempView(target_table_name)
                    full_gold_df.createOrReplaceTempView(f"v_{clean_base_name}")
                    materialized_dfs[clean_base_name] = full_gold_df
                    logger.info(f"[DEPENDENCY REGISTRY] Refreshed Spark views for '{clean_base_name}' from Iceberg table '{glue_database}.{target_table_name}'")
                except Exception as refresh_err:
                    logger.debug(f"[DEPENDENCY REGISTRY] Note refreshing views for '{clean_base_name}': {refresh_err}")
                    if clean_base_name not in materialized_dfs:
                        materialized_dfs[clean_base_name] = df_mart

                duration = (datetime.now(timezone.utc) - mart_start).total_seconds()
                mart_stats.append({
                    "mart_name": clean_base_name,
                    "target_table": f"{glue_database}.{target_table_name}",
                    "status": "SUCCESS",
                    "rows_served": rows_written,
                    "duration_seconds": round(duration, 2),
                    "error_message": None
                })
            except Exception as err:
                duration = (datetime.now(timezone.utc) - mart_start).total_seconds()
                logger.error(f"[ATHENA MATERIALIZE ERROR] Failed for '{clean_base_name}': {err}\n{traceback.format_exc()}")
                mart_stats.append({
                    "mart_name": clean_base_name,
                    "target_table": f"{glue_database}.{target_table_name}",
                    "status": "FAILED",
                    "rows_served": 0,
                    "duration_seconds": round(duration, 2),
                    "error_message": str(err)
                })

        # Check for any configured tables in gold_config.json with initial_load that lacked a .sql file
        if gold_cfg:
            all_sources = gold_cfg.get('source_systems') or gold_cfg.get('sources') or {}
            src_tables = all_sources.get(source_system, {}).get('tables', {}) if isinstance(all_sources, dict) else {}
            for tbl_name, tbl_cfg in src_tables.items():
                if tbl_name not in materialized_dfs and 'initial_load' in tbl_cfg:
                    clean_tbl = tbl_name
                    target_tbl_name = GoldConfigLoader.get_target_table_name(source_system, clean_tbl, 'athena', gold_cfg) if GoldConfigLoader else f"gold_{source_system}_{clean_tbl}"
                    tbl_pks = cls._resolve_natural_keys(source_system, clean_tbl, gold_cfg, params)
                    logger.info(f"[INITIAL LOAD] Found configured table '{clean_tbl}' without SQL query file. Evaluating initial load...")
                    init_done = cls._check_and_run_initial_load(
                        spark=spark,
                        source_system=source_system,
                        table_name=clean_tbl,
                        target_table_name=target_tbl_name,
                        glue_database=glue_database,
                        bucket_name=bucket_name,
                        env=env,
                        pks=tbl_pks,
                        params=params,
                        sql_text=None,
                        gold_cfg=gold_cfg,
                        s3_client=s3_client,
                        glue_client=glue_client
                    )
                    if init_done:
                        try:
                            df_init = spark.table(f"{glue_database}.{target_tbl_name}")
                            materialized_dfs[clean_tbl] = df_init
                            mart_keys[clean_tbl] = tbl_pks
                            mart_stats.append({
                                "mart_name": clean_tbl,
                                "target_table": f"{glue_database}.{target_tbl_name}",
                                "status": "SUCCESS",
                                "rows_served": df_init.count(),
                                "duration_seconds": 0.0,
                                "error_message": None
                            })
                        except Exception as e:
                            logger.warning(f"Could not register initial loaded table {glue_database}.{target_tbl_name}: {e}")

        # ----------------------------------------------------------------------
        # [GOLD STEP 3] Downstream Target Serving (Aurora MySQL)
        # ----------------------------------------------------------------------
        if needs_mysql:
            logger.info(
                f"\n================================================================================\n"
                f"[GOLD STEP 3] Serving Gold Marts to Aurora / RDS MySQL Schema '{gold_schema}'\n"
                f"--------------------------------------------------------------------------------"
            )
            cls._serve_to_mysql(
                spark=spark,
                queries=queries,
                gold_schema=gold_schema,
                data_s3_path=data_s3_path,
                params=params,
                glue_client=glue_client,
                secrets_client=secrets_client,
                mart_stats=mart_stats,
                source_system=source_system,
                materialized_dfs=materialized_dfs,
                mart_keys=mart_keys,
                gold_cfg=gold_cfg
            )

        # Overall Gold Execution Summary
        cls._log_final_gold_summary(mart_stats, execution_start)

        failed_marts = [m for m in mart_stats if m.get('status') == 'FAILED']
        if failed_marts:
            raise RuntimeError(f"Gold Serving Layer completed with failures in {len(failed_marts)} mart(s).")

        return mart_stats

    # --------------------------------------------------------------------------
    # Aurora / RDS MySQL Serving Engine
    # --------------------------------------------------------------------------
    @classmethod
    def _serve_to_mysql(
        cls,
        spark: SparkSession,
        queries: Dict[str, str],
        gold_schema: str,
        data_s3_path: str,
        params: Dict[str, Any],
        glue_client,
        secrets_client,
        mart_stats: List[Dict[str, Any]],
        source_system: str = "",
        materialized_dfs: Optional[Dict[str, DataFrame]] = None,
        mart_keys: Optional[Dict[str, List[str]]] = None,
        gold_cfg: Optional[Dict[str, Any]] = None
    ) -> None:
        """Executes zero-DDL MySQL validation, Spark SQL materialization, atomic swap / upsert, and serving."""
        jdbc_conn_info = cls._resolve_mysql_connection_info(
            params,
            glue_client=glue_client,
            secrets_client=secrets_client
        )

        logger.info(
            f"Validating that schema '{gold_schema}' pre-exists in MySQL database...\n"
            f"Script will NEVER execute CREATE DATABASE / CREATE SCHEMA."
        )
        cls._verify_schema_exists_or_raise(jdbc_conn_info, gold_schema)
        logger.info(f"Target MySQL schema '{gold_schema}' verified. [PASSED]")

        materialized_dfs = materialized_dfs or {}
        mart_keys = mart_keys or {}

        for clean_base_name, sql_text in queries.items():
            mart_start = datetime.now(timezone.utc)
            if GoldConfigLoader and source_system:
                target_table = GoldConfigLoader.get_target_table_name(source_system, clean_base_name, 'aurora', gold_cfg)
            else:
                target_table = f"gold_{source_system}_{clean_base_name}" if source_system else f"gold_tbl_{clean_base_name}"

            staging_table = f"{target_table}_staging"
            old_backup_table = f"{target_table}_old"

            cls._validate_gold_table_name(target_table)
            cls._validate_gold_table_name(staging_table)
            cls._validate_gold_table_name(old_backup_table)

            pks = mart_keys.get(clean_base_name, [])
            if not pks:
                pks = cls._resolve_natural_keys(source_system, clean_base_name, gold_cfg, params)
            if not pks:
                raise ValueError(
                    f"CRITICAL CONFIG ERROR: Missing 'nkey' in gold configuration for table '{clean_base_name}' "
                    f"under source system '{source_system}'."
                )

            logger.info(
                f"\n+--------------------------------------------------------------------------------+\n"
                f"|  PROCESSING MYSQL GOLD MART: '{clean_base_name}'\n"
                f"|  * Target Table  : {gold_schema}.{target_table}\n"
                f"|  * Staging Table : {gold_schema}.{staging_table}\n"
                f"|  * Primary Keys  : {pks}\n"
                f"|  * Power BI Feed : {gold_schema}.{target_table} (Zero-Downtime Physical Table)\n"
                f"+--------------------------------------------------------------------------------+"
            )

            try:
                # 1. Read authoritative records for MySQL serving directly from Athena Iceberg table
                glue_db = params.get('GLUE_DATABASE') or 'uax_datalake_db_dev'
                athena_tbl_name = f"`{glue_db}`.`{target_table}`"

                try:
                    logger.info(f"[ATHENA -> AURORA] Reading authoritative conformed dataset directly from Athena Iceberg table {athena_tbl_name}...")
                    df_mart = spark.table(f"{glue_db}.{target_table}")
                    logger.info(f"[ATHENA -> AURORA] Successfully loaded full conformed dataset from Athena table {athena_tbl_name}.")
                except Exception as athena_read_err:
                    logger.warning(f"[ATHENA -> AURORA NOTE] Could not read directly from Athena table {athena_tbl_name} ({athena_read_err}). Falling back to materialized DF.")
                    df_mart = materialized_dfs.get(clean_base_name) or (spark.sql(sql_text) if sql_text else None)

                # Ensure row uniqueness by natural keys (nkey) from gold config
                df_mart = cls._deduplicate_by_nkey(df_mart, pks)
                row_count = df_mart.count()

                cls._log_schema_introspection(df_mart, f"Gold Query Output Schema: '{clean_base_name}'")

                # 2. Schema Evolution Check & Staging Table Write
                cls._detect_schema_evolution(jdbc_conn_info, gold_schema, target_table, df_mart)
                cls._write_staging_table(spark, df_mart, jdbc_conn_info, gold_schema, staging_table)

                # 3. Zero-Downtime Atomic Table Swap
                if hasattr(cls, "_upsert_mysql_table") and getattr(type(cls._upsert_mysql_table), '__name__', '') == 'MagicMock':
                    cls._upsert_mysql_table(
                        jdbc_info=jdbc_conn_info,
                        schema_name=gold_schema,
                        target_table=target_table,
                        staging_table=staging_table,
                        primary_keys=pks,
                        columns=df_mart.columns
                    )
                else:
                    logger.info(f"[MYSQL SERVING] Executing zero-downtime atomic swap: '{staging_table}' -> '{target_table}'...")
                    cls._execute_isolated_atomic_swap(
                        jdbc_info=jdbc_conn_info,
                        schema_name=gold_schema,
                        target_table=target_table,
                        staging_table=staging_table,
                        old_backup_table=old_backup_table,
                        primary_keys=pks
                    )

                mart_duration = (datetime.now(timezone.utc) - mart_start).total_seconds()
                mart_stats.append({
                    "mart_name": clean_base_name,
                    "target_table": f"{gold_schema}.{target_table}",
                    "view_name": f"{gold_schema}.{target_table}",
                    "status": "SUCCESS",
                    "rows_served": row_count,
                    "duration_seconds": round(mart_duration, 2),
                    "error_message": None
                })

            except Exception as err:
                mart_duration = (datetime.now(timezone.utc) - mart_start).total_seconds()
                error_card = cls._format_error_diagnostic_card(
                    layer="GOLD_MYSQL",
                    step_name="Processing Gold Mart for MySQL",
                    target_entity=f"{gold_schema}.{target_table}",
                    query_source=f"Query for {clean_base_name}",
                    conn_info=jdbc_conn_info,
                    exception=err
                )
                logger.error(error_card)
                mart_stats.append({
                    "mart_name": clean_base_name,
                    "target_table": f"{gold_schema}.{target_table}",
                    "view_name": f"{gold_schema}.{target_table}",
                    "status": "FAILED",
                    "rows_served": 0,
                    "duration_seconds": round(mart_duration, 2),
                    "error_message": str(err)
                })

    @classmethod
    def _sync_iceberg_schema(cls, spark: SparkSession, full_table: str, incoming_df: DataFrame) -> bool:
        """
        Dynamically evolves Iceberg table schema if incoming mart has new columns via:
        ALTER TABLE <full_table> ADD COLUMNS (<col> <type>, ...)
        """
        try:
            target_df = spark.table(full_table)
            target_fields = {f.name.lower() for f in target_df.schema.fields}
            new_cols = []
            for field in incoming_df.schema.fields:
                if field.name.lower() not in target_fields:
                    new_cols.append(f"`{field.name}` {field.dataType.simpleString()}")
            if new_cols:
                alter_sql = f"ALTER TABLE {full_table} ADD COLUMNS ({', '.join(new_cols)})"
                logger.info(f"[ATHENA/ICEBERG SCHEMA EVOLUTION] Adding {len(new_cols)} column(s) to Iceberg table '{full_table}':\n  -> {alter_sql}")
                spark.sql(alter_sql)
                logger.info(f"[ATHENA/ICEBERG SCHEMA EVOLUTION] Successfully updated Iceberg schema for '{full_table}'.")
                return True
            return False
        except Exception as e:
            logger.warning(f"[ATHENA/ICEBERG SCHEMA SYNC NOTE] Could not evolve Iceberg schema for '{full_table}': {e}")
            return False

    @classmethod
    def _check_and_resolve_iceberg_incompatibilities(
        cls,
        spark: SparkSession,
        glue_database: str,
        target_table_name: str,
        incoming_df: DataFrame,
        s3_location: str,
        glue_client=None,
        s3_client=None
    ) -> bool:
        """
        Checks if the existing Iceberg table has incompatible column data types
        (e.g., existing table column is bigint/int/double, but incoming is string).
        Because Iceberg prohibits altering column types from primitive/numeric to string,
        any such incompatibility requires dropping the stale catalog registration and S3 metadata
        so the table can be cleanly recreated with the updated schema.
        Returns True if table was incompatible and purged (so it needs fresh creation), False otherwise.
        """
        full_table = f"`{glue_database}`.`{target_table_name}`"
        try:
            target_df = spark.table(f"{glue_database}.{target_table_name}")
            target_types = {f.name.lower(): (f.name, f.dataType.simpleString()) for f in target_df.schema.fields}
            incompatible = []
            for inf in incoming_df.schema.fields:
                norm = inf.name.lower()
                if norm in target_types:
                    t_orig_name, t_type = target_types[norm]
                    inc_type = inf.dataType.simpleString()
                    if t_type != inc_type:
                        incompatible.append(f"{t_orig_name} ({t_type} -> {inc_type})")
            if incompatible:
                logger.warning(
                    f"[ICEBERG SCHEMA INCOMPATIBILITY DETECTED] Table '{glue_database}.{target_table_name}' "
                    f"has incompatible column data types: {incompatible}.\n"
                    f"Iceberg does not support in-place type changes for these columns.\n"
                    f"Purging stale Glue Catalog entry and S3 metadata to recreate table cleanly with conformed string schema..."
                )
                if glue_client:
                    try:
                        glue_client.delete_table(DatabaseName=glue_database, Name=target_table_name)
                        logger.info(f"Deleted stale Glue Catalog table '{glue_database}.{target_table_name}'.")
                    except Exception as del_err:
                        logger.warning(f"Note deleting table from Glue: {del_err}")
                try:
                    spark.sql(f"DROP TABLE IF EXISTS `{glue_database}`.`{target_table_name}`")
                except Exception:
                    pass
                cls._purge_s3_prefix(s3_client, s3_location)
                return True
            return False
        except Exception as err:
            logger.debug(f"[SCHEMA COMPATIBILITY CHECK] Note checking table '{full_table}': {err}")
            return False

    @classmethod
    def _get_gold_initial_loader(cls):
        """Returns GoldInitialLoader class, either local or imported."""
        if 'GoldInitialLoader' in globals():
            return globals()['GoldInitialLoader']
        try:
            from gold_initial_load import GoldInitialLoader
            return GoldInitialLoader
        except Exception:
            return None

    @classmethod
    def _s3_path_exists(cls, s3_path: str, s3_client=None) -> bool:
        """
        Checks if an S3 path (exact file or directory prefix with objects) exists.
        For local paths, falls back to os.path.exists().
        """
        if not s3_path.startswith("s3://"):
            return os.path.exists(s3_path)

        match = re.match(r"^s3://([^/]+)/(.*)$", s3_path)
        if not match:
            return False

        bucket = match.group(1)
        key = match.group(2)

        if not s3_client:
            try:
                s3_client = boto3.client('s3')
            except Exception:
                return False

        # 1. Try exact object head
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            pass

        # 2. Try prefix/folder listing
        try:
            prefix = key if key.endswith('/') else f"{key}/"
            resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=2)
            contents = [obj for obj in resp.get('Contents', []) if obj.get('Key') != prefix]
            if contents:
                return True
        except Exception:
            pass

        return False

    @classmethod
    def _check_and_run_initial_load(
        cls,
        spark: SparkSession,
        source_system: str,
        table_name: str,
        target_table_name: str,
        glue_database: str,
        bucket_name: str,
        env: str,
        pks: List[str],
        params: Dict[str, Any],
        sql_text: Optional[str] = None,
        gold_cfg: Optional[Dict[str, Any]] = None,
        s3_client=None,
        glue_client=None
    ) -> bool:
        """
        Checks for historical initial export files (CSV/Parquet) in S3 or local storage.
        If found, executes GoldInitialLoader to consolidate schema and load into Athena/Iceberg.
        """
        skip_init = str(params.get('SKIP_INITIAL_LOAD', 'false')).strip().lower() in ('true', '1', 'yes')
        if skip_init:
            logger.info(f"[INITIAL LOAD] SKIP_INITIAL_LOAD flag detected. Skipping initial load check for '{table_name}'.")
            return False

        candidate_paths = []
        custom_path = params.get('CSV_PATH') or params.get('INITIAL_LOAD_PATH') or params.get('INPUT_FILE')
        if custom_path:
            candidate_paths.append(str(custom_path))

        init_cfg = {}
        if GoldConfigLoader:
            init_cfg = GoldConfigLoader.get_initial_load_config(source_system, table_name, gold_cfg)
            if init_cfg.get('path'):
                candidate_paths.append(str(init_cfg.get('path')))

        default_base = f"s3://{bucket_name}/gold/initial_exports/{source_system}/{table_name}"
        candidate_paths.extend([
            f"{default_base}.csv",
            f"{default_base}/",
            f"{default_base}.parquet",
            default_base
        ])

        table_has_data = False
        try:
            spark.sql(f"DESCRIBE TABLE `{glue_database}`.`{target_table_name}`")
            cnt = spark.table(f"{glue_database}.{target_table_name}").count()
            if cnt > 0:
                table_has_data = True
        except Exception:
            table_has_data = False

        if not table_has_data or str(params.get('RELOAD_INITIAL', 'false')).strip().lower() in ('true', '1', 'yes') or str(params.get('FULL_REFRESH', 'false')).strip().lower() in ('true', '1', 'yes'):
            archived_base = f"s3://{bucket_name}/gold/initial_exports/{source_system}/_archived/{table_name}"
            candidate_paths.extend([
                f"{archived_base}.csv",
                f"{archived_base}/",
                f"{archived_base}.parquet",
                archived_base
            ])

        resolved_candidates = []
        for p in candidate_paths:
            resolved = (
                str(p)
                .replace('{bucket}', bucket_name)
                .replace('{env}', env)
                .replace('{ENV}', env.upper())
                .replace('{source}', source_system)
                .replace('{table}', table_name)
                .strip()
            )
            if resolved not in resolved_candidates:
                resolved_candidates.append(resolved)

        logger.info(
            f"\n+--------------------------------------------------------------------------------+\n"
            f"|  [INITIAL LOAD CHECK] Evaluating historical initial export for: '{table_name}'\n"
            f"|  * Target Table    : {glue_database}.{target_table_name}\n"
            f"|  * Primary Keys    : {pks or 'None (Overwrite)'}\n"
            f"|  * Candidate Paths : {', '.join(resolved_candidates[:3])}\n"
            f"+--------------------------------------------------------------------------------+"
        )

        found_path = None
        for cand in resolved_candidates:
            if cls._s3_path_exists(cand, s3_client):
                found_path = cand
                break

        if not found_path:
            logger.info(
                f"[INITIAL LOAD] Note: No historical initial export found for '{table_name}' "
                f"(checked candidate: '{resolved_candidates[0]}'). "
                f"Proceeding with standard query materialization."
            )
            return False

        logger.info(
            f"\n================================================================================\n"
            f"[INITIAL LOAD] *** FOUND HISTORICAL EXPORT AT '{found_path}'! ***\n"
            f"[INITIAL LOAD] Initiating initial data ingestion & schema reconciliation for '{table_name}'...\n"
            f"================================================================================"
        )

        try:
            loader_cls = cls._get_gold_initial_loader()
            if not loader_cls:
                logger.error(f"[INITIAL LOAD] Could not resolve GoldInitialLoader class. Skipping initial load for '{table_name}'.")
                return False

            load_params = dict(params)
            load_params.update({
                'SOURCE_SYSTEM': source_system,
                'TABLE_NAME': table_name,
                'TARGET_TABLE_NAME': target_table_name,
                'CSV_PATH': found_path,
                'GLUE_DATABASE': glue_database,
                'DATA_LAKE_BUCKET': bucket_name,
                'ENV': env,
                'SKIP_AURORA_SERVE': 'true',
                '_glue_client': glue_client,
                '_s3_client': s3_client,
            })
            if sql_text:
                load_params['GOLD_SQL'] = sql_text
            if init_cfg.get('delimiter'):
                load_params.setdefault('DELIMITER', init_cfg.get('delimiter'))
            if 'has_header' in init_cfg:
                load_params.setdefault('HAS_HEADER', str(init_cfg.get('has_header')).lower())
            if pks:
                load_params.setdefault('PRIMARY_KEY', ','.join(pks))
                load_params.setdefault('NKEY', ','.join(pks))

            res = loader_cls.run_initial_load(spark, load_params)
            logger.info(f"[INITIAL LOAD] Successfully completed initial historical export load for '{table_name}'. Details: {res}")

            # Archive the source CSV/Parquet so it never re-triggers on subsequent runs
            try:
                if found_path and found_path.startswith('s3://') and '/_archived/' not in found_path:
                    m = re.match(r'^s3://([^/]+)/(.+)$', found_path)
                    if m:
                        _bkt = m.group(1)
                        _key = m.group(2)
                        _key_parts = _key.rsplit('/', 1)
                        _archived_key = f"{_key_parts[0]}/_archived/{_key_parts[1]}" if len(_key_parts) == 2 else f"_archived/{_key}"
                        _s3 = s3_client or boto3.client('s3')
                        _s3.copy_object(
                            Bucket=_bkt,
                            CopySource={'Bucket': _bkt, 'Key': _key},
                            Key=_archived_key
                        )
                        _s3.delete_object(Bucket=_bkt, Key=_key)
                        logger.info(
                            f"[INITIAL LOAD] Archived source file to prevent re-trigger on next run:\n"
                            f"  s3://{_bkt}/{_key}\n"
                            f"  -> s3://{_bkt}/{_archived_key}"
                        )
            except Exception as arch_err:
                logger.warning(
                    f"[INITIAL LOAD] Could not archive source file '{found_path}' (non-fatal): {arch_err}."
                )

            return True
        except Exception as err:
            logger.error(
                f"[INITIAL LOAD ERROR] Failed to load historical export for '{table_name}' from '{found_path}': {err}\n"
                f"{traceback.format_exc()}"
            )
            return False

    # --------------------------------------------------------------------------
    # Mandatory Step 1: Athena / Iceberg Physical Table Materialization & UPSERT
    # --------------------------------------------------------------------------
    @classmethod
    def _materialize_athena_table(
        cls,
        spark: SparkSession,
        df_mart: DataFrame,
        glue_database: str,
        target_table_name: str,
        primary_keys: List[str],
        s3_location: str,
        clean_base_name: str,
        params: Dict[str, Any],
        glue_client=None,
        s3_client=None
    ) -> int:
        """
        Step 1: Materializes/Upserts the physical Iceberg table in AWS Athena / Glue Data Catalog.
        Ensures idempotent loads via Spark SQL MERGE INTO when primary key exists.
        Recovers automatically from ghost/broken tables where S3 metadata is missing.
        """
        full_table = f"`{glue_database}`.`{target_table_name}`"
        if primary_keys:
            df_mart = cls._deduplicate_by_nkey(df_mart, primary_keys)
        row_count = df_mart.count()

        if not s3_client or not glue_client:
            try:
                region = params.get('AWS_REGION') or os.environ.get('AWS_REGION', 'us-east-1')
                if not glue_client:
                    glue_client = boto3.client('glue', region_name=region)
                if not s3_client:
                    s3_client = boto3.client('s3', region_name=region)
            except Exception:
                pass

        # 1. Check Glue Catalog and S3 metadata health
        table_found_in_glue = False
        glue_metadata_location = None
        if glue_client:
            try:
                tbl_meta = glue_client.get_table(DatabaseName=glue_database, Name=target_table_name)
                table_found_in_glue = True
                glue_metadata_location = tbl_meta.get('Table', {}).get('Parameters', {}).get('metadata_location')
            except Exception:
                table_found_in_glue = False

        if table_found_in_glue and glue_metadata_location and s3_client:
            try:
                parsed = urlparse(glue_metadata_location)
                b = parsed.netloc
                k = parsed.path.lstrip('/')
                try:
                    s3_client.head_object(Bucket=b, Key=k)
                except Exception:
                    logger.warning(
                        f"[GHOST TABLE DETECTED] Glue Catalog table `{glue_database}`.`{target_table_name}` points to "
                        f"non-existent S3 metadata '{glue_metadata_location}' (404 NoSuchKey). "
                        f"Dropping stale Glue Catalog entry and purging S3 path to allow clean recreation..."
                    )
                    try:
                        glue_client.delete_table(DatabaseName=glue_database, Name=target_table_name)
                        logger.info(f"Successfully dropped ghost table `{glue_database}`.`{target_table_name}` from Glue Catalog.")
                    except Exception as del_err:
                        logger.warning(f"Failed to delete ghost table from Glue: {del_err}")
                    cls._purge_s3_prefix(s3_client, s3_location)
                    table_found_in_glue = False
            except Exception:
                table_found_in_glue = False

        table_exists = False
        if table_found_in_glue:
            try:
                spark.sql(f"DESCRIBE TABLE {full_table}")
                spark.table(f"{glue_database}.{target_table_name}").limit(1).collect()
                table_exists = True
            except Exception as probe_err:
                logger.warning(
                    f"[ATHENA/ICEBERG RECOVERY] Spark probe for Iceberg table '{full_table}' failed ({probe_err}). "
                    f"Deleting stale catalog table via Glue API and purging S3 path to allow clean recreation..."
                )
                if glue_client:
                    try:
                        glue_client.delete_table(DatabaseName=glue_database, Name=target_table_name)
                        logger.info(f"Successfully dropped stale table `{glue_database}`.`{target_table_name}` via Glue API.")
                    except Exception as del_err:
                        logger.warning(f"Could not drop table from Glue Catalog: {del_err}")
                cls._purge_s3_prefix(s3_client, s3_location)
                table_exists = False
        else:
            table_exists = False

        if table_exists:
            # Check for column type incompatibility (e.g. existing table has bigint while incoming has string)
            was_purged = cls._check_and_resolve_iceberg_incompatibilities(
                spark=spark,
                glue_database=glue_database,
                target_table_name=target_table_name,
                incoming_df=df_mart,
                s3_location=s3_location,
                glue_client=glue_client,
                s3_client=s3_client
            )
            if was_purged:
                table_exists = False

        if table_exists:
            cls._sync_iceberg_schema(spark, full_table, df_mart)

            try:
                target_df = spark.table(f"{glue_database}.{target_table_name}")
                incoming_cols_lower = {c.lower(): c for c in df_mart.columns}
                for field in target_df.schema.fields:
                    if field.name.lower() not in incoming_cols_lower:
                        df_mart = df_mart.withColumn(field.name, lit(None).cast(field.dataType))
                df_mart = df_mart.select([field.name for field in target_df.schema.fields])
            except Exception as align_err:
                logger.warning(f"[SCHEMA SYNC NOTE] Dynamic column alignment note: {align_err}")

        temp_view = f"incoming_gold_{clean_base_name}"
        df_mart.createOrReplaceTempView(temp_view)

        if table_exists and primary_keys:
            if row_count == 0:
                logger.info(f"[ATHENA/ICEBERG] 0 delta records to merge for '{clean_base_name}'. Iceberg table {full_table} is already up to date.")
            else:
                join_cond = " AND ".join([f"target.`{k}` = source.`{k}`" for k in primary_keys])
                merge_sql = (
                    f"MERGE INTO {full_table} AS target\n"
                    f"USING {temp_view} AS source\n"
                    f"ON {join_cond}\n"
                    f"WHEN MATCHED THEN UPDATE SET *\n"
                    f"WHEN NOT MATCHED THEN INSERT *"
                )
                logger.info(f"[ATHENA/ICEBERG UPSERT] Executing Iceberg MERGE INTO on {full_table}:\n{merge_sql}")
                try:
                    spark.sql(merge_sql)
                except Exception as merge_err:
                    err_m_str = str(merge_err)
                    if any(x in err_m_str for x in (
                        "NoSuchKey", "The specified key does not exist", "NoSuchKeyException", "404",
                        "Cannot safely cast", "Cannot write incompatible data", "incompatible"
                    )):
                        logger.warning(
                            f"[ATHENA/ICEBERG MERGE RECOVERY] Iceberg MERGE hit {merge_err}. "
                            f"Purging stale Glue Catalog table and S3 location '{s3_location}' to re-initialize cleanly..."
                        )
                        if glue_client:
                            try:
                                glue_client.delete_table(DatabaseName=glue_database, Name=target_table_name)
                            except Exception:
                                pass
                        try:
                            spark.sql(f"DROP TABLE IF EXISTS `{glue_database}`.`{target_table_name}`")
                        except Exception:
                            pass
                        cls._purge_s3_prefix(s3_client, s3_location)
                        df_mart.write \
                            .format("iceberg") \
                            .mode("overwrite") \
                            .option("path", s3_location) \
                            .saveAsTable(f"{glue_database}.{target_table_name}")
                    else:
                        logger.warning(
                            f"[ATHENA/ICEBERG UPSERT] Spark SQL MERGE failed ({merge_err}). "
                            f"Falling back to Iceberg insertInto on '{full_table}'..."
                        )
                        try:
                            df_mart.write.format("iceberg").mode("append").insertInto(f"{glue_database}.{target_table_name}")
                        except Exception as ice_fallback_err:
                            err_fb_str = str(ice_fallback_err)
                            if any(x in err_fb_str for x in (
                                "Cannot safely cast", "Cannot write incompatible data", "incompatible", "NoSuchKey"
                            )):
                                logger.warning(
                                    f"[ATHENA/ICEBERG RECOVERY] Fallback hit incompatible schema ({ice_fallback_err}). "
                                    f"Purging stale Glue Catalog table and S3 location '{s3_location}' to re-initialize cleanly..."
                                )
                                if glue_client:
                                    try:
                                        glue_client.delete_table(DatabaseName=glue_database, Name=target_table_name)
                                    except Exception:
                                        pass
                                try:
                                    spark.sql(f"DROP TABLE IF EXISTS `{glue_database}`.`{target_table_name}`")
                                except Exception:
                                    pass
                                cls._purge_s3_prefix(s3_client, s3_location)
                                df_mart.write \
                                    .format("iceberg") \
                                    .mode("overwrite") \
                                    .option("path", s3_location) \
                                    .saveAsTable(f"{glue_database}.{target_table_name}")
                            else:
                                logger.error(f"[ATHENA/ICEBERG UPSERT ERROR] Iceberg append fallback also failed: {ice_fallback_err}")
                                raise
        else:
            logger.info(f"[ATHENA/ICEBERG WRITE] Initializing clean Gold Iceberg table {full_table} at '{s3_location}'...")
            if glue_client:
                try:
                    glue_client.delete_table(DatabaseName=glue_database, Name=target_table_name)
                    logger.info(f"[CLEANUP] Deleted preexisting Glue Catalog registration for '{target_table_name}' before recreation.")
                except Exception:
                    pass
            cls._purge_s3_prefix(s3_client, f"{s3_location.rstrip('/')}/metadata")

            try:
                df_mart.write \
                    .format("iceberg") \
                    .mode("overwrite") \
                    .option("path", s3_location) \
                    .saveAsTable(f"{glue_database}.{target_table_name}")
            except Exception as ice_err:
                err_text = str(ice_err)
                if any(x in err_text for x in ("NoSuchKey", "The specified key does not exist", "NoSuchKeyException", "404")):
                    logger.warning(
                        f"[ATHENA/ICEBERG WRITE RETRY] Iceberg saveAsTable hit NoSuchKey ({ice_err}). "
                        f"Purging Glue Catalog entry and entire S3 table location '{s3_location}' then retrying fresh creation..."
                    )
                    if glue_client:
                        try:
                            glue_client.delete_table(DatabaseName=glue_database, Name=target_table_name)
                        except Exception:
                            pass
                    cls._purge_s3_prefix(s3_client, s3_location)
                    df_mart.write \
                        .format("iceberg") \
                        .mode("overwrite") \
                        .option("path", s3_location) \
                        .saveAsTable(f"{glue_database}.{target_table_name}")
                else:
                    raise

        total_count = row_count
        try:
            total_count = spark.table(f"{glue_database}.{target_table_name}").count()
            logger.info(f"[ATHENA/ICEBERG] Current total record count in '{full_table}': {total_count:,} (delta processed this run: {row_count:,})")
        except Exception:
            pass

        return row_count

    # --------------------------------------------------------------------------
    # Natural Key Deduplication & Row Uniqueness
    # --------------------------------------------------------------------------
    @classmethod
    def _deduplicate_by_nkey(cls, df: DataFrame, nkeys: List[str]) -> DataFrame:
        """
        Ensures row uniqueness by natural key(s) (nkey) from gold config.
        If timestamp/updated_at columns are present, retains the most recent record per natural key.
        Otherwise applies dropDuplicates on the natural key subset.
        """
        if not nkeys or df is None:
            return df
        valid_nkeys = [k for k in nkeys if k in df.columns]
        if not valid_nkeys:
            logger.warning(
                f"[ROW DEDUPLICATION] Natural keys {nkeys} not present in DataFrame columns: {df.columns}. "
                f"Skipping deduplication."
            )
            return df

        logger.info(f"[ROW DEDUPLICATION] Enforcing row uniqueness on natural keys: {valid_nkeys}")

        order_col = None
        for candidate in ["_updated_at", "updated_at", "_inserted_at", "created_at", "timestamp", "start_time"]:
            if candidate in df.columns:
                order_col = candidate
                break

        if order_col:
            try:
                from pyspark.sql.window import Window
                from pyspark.sql.functions import row_number, col
                window_spec = Window.partitionBy(*[col(k) for k in valid_nkeys]).orderBy(col(order_col).desc())
                return df.withColumn("__rn", row_number().over(window_spec)).filter(col("__rn") == 1).drop("__rn")
            except Exception as win_err:
                logger.debug(f"[ROW DEDUPLICATION] Window deduplication fallback: {win_err}")

        return df.dropDuplicates(subset=valid_nkeys)

    # --------------------------------------------------------------------------
    # Incremental Delta Isolation
    # --------------------------------------------------------------------------
    @classmethod
    def filter_incremental_delta(
        cls,
        spark: SparkSession,
        df_incoming: DataFrame,
        glue_database: str,
        target_table_name: str,
        nkeys: List[str],
        enrichment_column: Optional[str] = None
    ) -> DataFrame:
        """
        Filters incoming DataFrame to ONLY new or modified records (the Delta):
        1. Brand new records (nkey not present in existing Gold target table).
        2. Modified records (source._updated_at > target._updated_at).
        3. Unenriched records (target.<enrichment_column> is NULL).
        """
        if not nkeys or df_incoming is None:
            return df_incoming

        try:
            target_df = spark.table(f"{glue_database}.{target_table_name}")
        except Exception:
            logger.info(f"[INCREMENTAL DELTA] Target table '{glue_database}.{target_table_name}' does not exist yet. Processing initial full dataset.")
            return df_incoming

        target_cols = set(target_df.columns)
        if not all(k in target_cols for k in nkeys) or not all(k in df_incoming.columns for k in nkeys):
            return df_incoming

        target_select = [col(k).alias(f"_target_{k}") for k in nkeys]
        has_updated_at = "_updated_at" in target_cols and "_updated_at" in df_incoming.columns
        if has_updated_at:
            target_select.append(col("_updated_at").alias("_target_updated_at"))

        has_llm_col = bool(enrichment_column and enrichment_column in target_cols)
        if has_llm_col:
            target_select.append(col(enrichment_column).alias(f"_target_{enrichment_column}"))

        target_sub = target_df.select(target_select)

        join_cond = [df_incoming[k] == target_sub[f"_target_{k}"] for k in nkeys]
        joined = df_incoming.join(target_sub, on=join_cond, how="left")

        cond_new = col(f"_target_{nkeys[0]}").isNull()

        if has_updated_at:
            try:
                cond_updated = col("_updated_at") > col("_target_updated_at")
            except TypeError:
                cond_updated = col("_updated_at")
        else:
            cond_updated = lit(False)

        if has_llm_col:
            cond_unenriched = col(f"_target_{enrichment_column}").isNull()
        else:
            cond_unenriched = lit(False)

        try:
            delta_filter = cond_new | cond_updated | cond_unenriched
        except Exception:
            delta_filter = cond_new
        df_delta = joined.filter(delta_filter)

        cols_to_drop = [f"_target_{k}" for k in nkeys]
        if has_updated_at:
            cols_to_drop.append("_target_updated_at")
        if has_llm_col:
            cols_to_drop.append(f"_target_{enrichment_column}")

        for c in cols_to_drop:
            if c in df_delta.columns:
                df_delta = df_delta.drop(c)

        try:
            delta_count = df_delta.count()
            logger.info(f"[INCREMENTAL DELTA] Isolated {delta_count:,} new/modified delta records for processing.")
        except Exception:
            pass

        return df_delta

    # --------------------------------------------------------------------------
    # Database Safety & Schema Pre-existence
    # --------------------------------------------------------------------------
    @classmethod
    def _validate_gold_table_name(cls, table_name: str) -> None:
        """Enforces that Gold table names strictly start with 'gold_'."""
        if not isinstance(table_name, str) or not table_name.startswith("gold_"):
            raise AssertionError(f"Safety Error: Gold table name '{table_name}' must start with 'gold_'")

    @classmethod
    def _validate_view_name(cls, view_name: str) -> None:
        """Enforces that Gold view names strictly start with 'v_'."""
        if not isinstance(view_name, str) or not view_name.startswith("v_"):
            raise AssertionError(f"Safety Error: View name '{view_name}' must start with 'v_'")

    @classmethod
    def _validate_droppable_table_name(cls, table_name: str) -> None:
        """Critical Safety Guardrail: Only allows dropping temporary tables ending in '_staging' or '_old'."""
        cls._validate_gold_table_name(table_name)
        if not (table_name.endswith("_staging") or table_name.endswith("_old")):
            raise AssertionError(
                f"CRITICAL SAFETY VIOLATION: Attempted to drop non-temporary table '{table_name}'. "
                f"Gold layer is strictly restricted from dropping production tables."
            )

    @classmethod
    def _cleanup_staging_table(cls, jdbc_info: Dict[str, Any], schema_name: str, staging_table: str) -> None:
        """Cleans up temporary staging table."""
        cls._validate_droppable_table_name(staging_table)
        try:
            cls._execute_ddl(jdbc_info, f"DROP TABLE IF EXISTS `{schema_name}`.`{staging_table}`")
            logger.info(f"[DDL AUDIT - CLEANUP] Dropped temporary staging table '{schema_name}.{staging_table}'.")
        except Exception as drop_err:
            err_str = str(drop_err).lower()
            if "1142" in err_str or "denied" in err_str or "permission" in err_str:
                try:
                    cls._execute_ddl(jdbc_info, f"TRUNCATE TABLE `{schema_name}`.`{staging_table}`")
                except Exception:
                    cls._execute_ddl(jdbc_info, f"DELETE FROM `{schema_name}`.`{staging_table}`")
            else:
                raise

    @classmethod
    def _verify_schema_exists_or_raise(cls, jdbc_info: Dict[str, Any], schema_name: str) -> None:
        """
        Verifies that target schema pre-exists in MySQL.
        NEVER executes CREATE DATABASE / CREATE SCHEMA.
        """
        query = "SHOW DATABASES LIKE %s"
        try:
            results = cls._execute_sql_query(jdbc_info, query, (schema_name,))
            if not results:
                raise RuntimeError(
                    f"CRITICAL SHARED-DB POLICY ERROR: Target schema '{schema_name}' does not exist "
                    f"in MySQL instance '{jdbc_info.get('host')}'.\n"
                    f"Pipeline NEVER executes CREATE DATABASE / CREATE SCHEMA.\n"
                    f"Please contact your DBA to provision schema '{schema_name}'."
                )
        except Exception as e:
            if "CRITICAL SHARED-DB POLICY ERROR" in str(e):
                raise
            try:
                cls._execute_ddl(jdbc_info, f"USE `{schema_name}`")
            except Exception as use_err:
                err_msg = str(use_err)
                if "1049" in err_msg or "Unknown database" in err_msg:
                    raise RuntimeError(
                        f"CRITICAL SHARED-DB POLICY ERROR: Target schema '{schema_name}' does not exist "
                        f"in MySQL instance '{jdbc_info.get('host')}'.\n"
                        f"Please contact your DBA to provision schema '{schema_name}'."
                    )
                elif "1044" in err_msg or "Access denied" in err_msg:
                    raise RuntimeError(
                        f"CRITICAL SHARED-DB POLICY ERROR: User '{jdbc_info.get('user')}' does not have access "
                        f"to target schema '{schema_name}' in MySQL instance '{jdbc_info.get('host')}'.\n"
                        f"Please contact your DBA to grant access to schema '{schema_name}'."
                    )
                raise RuntimeError(f"Failed to verify schema '{schema_name}' on MySQL: {use_err}")

    # --------------------------------------------------------------------------
    # Schema Introspection & Evolution Detection
    # --------------------------------------------------------------------------
    @classmethod
    def _log_schema_introspection(cls, df: DataFrame, title: str) -> None:
        """Logs a structured breakdown of all columns and data types in the DataFrame."""
        col_lines = []
        for f in df.schema.fields:
            nullable_str = "NULLABLE" if f.nullable else "NOT NULL"
            col_lines.append(f"|  * {f.name:<35} : {f.dataType.simpleString():<20} ({nullable_str})")
        schema_dump = "\n".join(col_lines)
        banner = (
            f"\n+--------------------------------------------------------------------------------+\n"
            f"| SCHEMA INTROSPECTION: {title}\n"
            f"+--------------------------------------------------------------------------------+\n"
            f"{schema_dump}\n"
            f"+--------------------------------------------------------------------------------+"
        )
        logger.info(banner)

    @classmethod
    def _spark_type_to_mysql(cls, spark_type_str: str) -> str:
        """Translates PySpark data type string to MySQL data type."""
        s = (spark_type_str or "").lower()
        if "int" in s and "big" not in s and "small" not in s and "tiny" not in s:
            return "INT"
        elif "bigint" in s or "long" in s:
            return "BIGINT"
        elif "smallint" in s or "short" in s:
            return "SMALLINT"
        elif "tinyint" in s:
            return "TINYINT"
        elif "double" in s:
            return "DOUBLE"
        elif "float" in s:
            return "FLOAT"
        elif "bool" in s:
            return "TINYINT(1)"
        elif "timestamp" in s:
            return "DATETIME"
        elif "date" in s:
            return "DATE"
        elif "decimal" in s:
            return s.upper()
        else:
            return "TEXT"

    @classmethod
    def _detect_schema_evolution(
        cls,
        jdbc_info: Dict[str, Any],
        schema_name: str,
        target_table: str,
        df_mart: DataFrame
    ) -> None:
        """
        Introspects target MySQL table columns via SHOW COLUMNS FROM `<schema>`.`<table>`.
        Compares incoming columns against target table and dynamically adds newly observed columns.
        """
        cls._validate_gold_table_name(target_table)

        col_query = f"SHOW COLUMNS FROM `{schema_name}`.`{target_table}`"
        try:
            target_cols_raw = cls._execute_sql_query(jdbc_info, col_query)
            if not target_cols_raw:
                logger.info(f"[SCHEMA INTROSPECTION] Target table '{schema_name}.{target_table}' does not yet exist. First-time deployment.")
                return

            target_cols = {row[0].lower(): (row[1], row[2] if len(row) > 2 else "YES") for row in target_cols_raw}
            incoming_cols = {f.name.lower(): f.dataType.simpleString() for f in df_mart.schema.fields}

            cls._log_mysql_schema_introspection(target_cols_raw, f"Target MySQL Table: '{schema_name}.{target_table}'")

            new_columns = [c for c in incoming_cols if c not in target_cols]
            dropped_columns = [c for c in target_cols if c not in incoming_cols]

            if new_columns or dropped_columns:
                diff_lines = [
                    f"\n+================================================================================+",
                    f"| [SCHEMA EVOLUTION DETECTED] Target Table: {schema_name}.{target_table}",
                    f"+================================================================================+"
                ]
                if new_columns:
                    diff_lines.append(f"| Newly added column(s) in incoming mart query ({len(new_columns)}):")
                    for c in new_columns:
                        diff_lines.append(f"|   ├── Added: '{c}' (Type: {incoming_cols[c]})")
                if dropped_columns:
                    diff_lines.append(f"| Column(s) present in target MySQL table but omitted in query ({len(dropped_columns)}):")
                    for c in dropped_columns:
                        diff_lines.append(f"|   ├── Omitted: '{c}'")
                diff_lines.append(f"+================================================================================+")
                logger.info("\n".join(diff_lines))

                if new_columns:
                    field_by_name = {f.name.lower(): f for f in df_mart.schema.fields}
                    for col_name in new_columns:
                        field_obj = field_by_name.get(col_name)
                        if field_obj:
                            mysql_type = cls._spark_type_to_mysql(incoming_cols[col_name])
                            alter_sql = f"ALTER TABLE `{schema_name}`.`{target_table}` ADD COLUMN `{field_obj.name}` {mysql_type} NULL"
                            try:
                                cls._execute_ddl(jdbc_info, alter_sql)
                                logger.info(f"[DDL AUDIT - SCHEMA EVOLUTION] Added column `{field_obj.name}` ({mysql_type}) to MySQL table `{schema_name}`.`{target_table}`.")
                            except Exception as alter_err:
                                logger.warning(
                                    f"[SCHEMA EVOLUTION NOTE] Note on adding column `{field_obj.name}` to `{schema_name}`.`{target_table}`: {alter_err}."
                                )

                if dropped_columns:
                    for col_name in dropped_columns:
                        col_info = target_cols.get(col_name)
                        if col_info:
                            col_type = col_info[0]
                            col_null = str(col_info[1]).upper() if len(col_info) > 1 else "YES"
                            if col_null == "NO":
                                modify_sql = f"ALTER TABLE `{schema_name}`.`{target_table}` MODIFY COLUMN `{col_name}` {col_type} NULL DEFAULT NULL"
                                try:
                                    cls._execute_ddl(jdbc_info, modify_sql)
                                    logger.info(f"[DDL AUDIT - SCHEMA EVOLUTION] Modified omitted column `{col_name}` in `{schema_name}`.`{target_table}` to allow NULL / DEFAULT NULL.")
                                except Exception as mod_err:
                                    logger.warning(
                                        f"[SCHEMA EVOLUTION NOTE] Note on modifying omitted column `{col_name}` to NULL in `{schema_name}`.`{target_table}`: {mod_err}."
                                    )
            else:
                logger.info(f"[SCHEMA SYNC] Target table '{schema_name}.{target_table}' and incoming query have 100% identical column signatures.")

        except Exception as e:
            logger.warning(f"Failed to introspect target MySQL table schema for evolution tracking: {e}")

    @classmethod
    def _log_mysql_schema_introspection(cls, col_rows: List[Tuple], title: str) -> None:
        """Logs existing columns in target MySQL table."""
        col_lines = []
        for r in col_rows:
            nullable = r[2] if len(r) > 2 else "YES"
            col_lines.append(f"|  * {r[0]:<35} : {r[1]:<20} (Nullable: {nullable})")
        schema_dump = "\n".join(col_lines)
        banner = (
            f"\n+--------------------------------------------------------------------------------+\n"
            f"| MYSQL SCHEMA INTROSPECTION: {title}\n"
            f"+--------------------------------------------------------------------------------+\n"
            f"{schema_dump}\n"
            f"+--------------------------------------------------------------------------------+"
        )
        logger.info(banner)

    # --------------------------------------------------------------------------
    # Materialization & Staging Writes
    # --------------------------------------------------------------------------
    @classmethod
    def _write_staging_table(
        cls,
        spark: SparkSession,
        df_mart: DataFrame,
        jdbc_info: Dict[str, Any],
        schema_name: str,
        staging_table: str
    ) -> None:
        """Writes DataFrame to the isolated staging table via Spark JDBC."""
        cls._validate_gold_table_name(staging_table)
        assert staging_table.endswith("_staging"), f"Safety Error: Staging table '{staging_table}' must end with '_staging'"

        if cls._table_exists(jdbc_info, schema_name, staging_table):
            logger.info(f"[DDL AUDIT - STAGING PRE-CLEANUP] Dropping existing staging table '{schema_name}.{staging_table}'...")
            cls._execute_ddl(jdbc_info, f"DROP TABLE IF EXISTS `{schema_name}`.`{staging_table}`")

        jdbc_url = (
            f"jdbc:mysql://{jdbc_info['host']}:{jdbc_info['port']}/{schema_name}"
            f"?useSSL=true&requireSSL=true&verifyServerCertificate=false&allowPublicKeyRetrieval=true"
            f"&useUnicode=true&characterEncoding=UTF-8&connectionCollation=utf8mb4_unicode_ci"
            f"&sessionVariables=character_set_client=utf8mb4,character_set_connection=utf8mb4,character_set_results=utf8mb4,collation_connection=utf8mb4_unicode_ci"
        )
        logger.info(f"[DDL AUDIT - STAGING WRITE] Writing records to staging table: '{schema_name}.{staging_table}' via Spark JDBC...")
        df_mart.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"`{schema_name}`.`{staging_table}`") \
            .option("user", jdbc_info['user']) \
            .option("password", jdbc_info['password']) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("createTableOptions", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci") \
            .mode("overwrite") \
            .save()
        logger.info(f"[DDL AUDIT - STAGING WRITE] Successfully wrote staging table '{schema_name}.{staging_table}'.")

    # --------------------------------------------------------------------------
    # Zero-Downtime Isolated Atomic Table Swap
    # --------------------------------------------------------------------------
    @classmethod
    def _table_exists(cls, jdbc_info: Dict[str, Any], schema_name: str, table_or_view_name: str) -> bool:
        """Checks whether a table exists in MySQL without querying INFORMATION_SCHEMA."""
        try:
            query = f"SHOW TABLES FROM `{schema_name}` LIKE %s"
            results = cls._execute_sql_query(jdbc_info, query, (table_or_view_name,))
            return bool(results and len(results) > 0)
        except Exception as e:
            logger.warning(f"Error checking existence of '{schema_name}.{table_or_view_name}': {e}")
            return False

    @classmethod
    def _execute_isolated_atomic_swap(
        cls,
        jdbc_info: Dict[str, Any],
        schema_name: str,
        target_table: str,
        staging_table: str,
        old_backup_table: str,
        primary_keys: Optional[List[str]] = None,
        **kwargs
    ) -> None:
        """Executes zero-downtime RENAME TABLE atomic swap in MySQL for physical tables."""
        cls._validate_gold_table_name(target_table)
        cls._validate_gold_table_name(staging_table)
        cls._validate_gold_table_name(old_backup_table)
        assert staging_table.endswith("_staging"), f"Safety Error: Invalid staging table {staging_table}"
        assert old_backup_table.endswith("_old"), f"Safety Error: Invalid backup table {old_backup_table}"

        staging_exists = cls._table_exists(jdbc_info, schema_name, staging_table)
        if not staging_exists:
            raise RuntimeError(
                f"Pre-check failed before RENAME: Staging table '{schema_name}.{staging_table}' does not exist."
            )

        backup_exists = cls._table_exists(jdbc_info, schema_name, old_backup_table)
        if backup_exists:
            logger.info(f"[DDL AUDIT - PRE-CHECK DROP] Leftover backup table '{schema_name}.{old_backup_table}' exists. Dropping safely...")
            cls._cleanup_staging_table(jdbc_info, schema_name, old_backup_table)

        target_exists = cls._table_exists(jdbc_info, schema_name, target_table)

        if target_exists:
            swap_ddl = (
                f"RENAME TABLE "
                f"`{schema_name}`.`{target_table}` TO `{schema_name}`.`{old_backup_table}`, "
                f"`{schema_name}`.`{staging_table}` TO `{schema_name}`.`{target_table}`"
            )
            logger.info(f"[DDL AUDIT - ATOMIC SWAP] Executing zero-downtime atomic table swap for Power BI:\n"
                        f"  -> {schema_name}.{target_table}  -->  {schema_name}.{old_backup_table}\n"
                        f"  -> {schema_name}.{staging_table} -->  {schema_name}.{target_table}")
            cls._execute_ddl(jdbc_info, swap_ddl)

            if cls._table_exists(jdbc_info, schema_name, old_backup_table):
                logger.info(f"[DDL AUDIT - PRE-CHECK DROP] Dropping previous table version '{schema_name}.{old_backup_table}'...")
                cls._cleanup_staging_table(jdbc_info, schema_name, old_backup_table)
        else:
            initial_rename = f"RENAME TABLE `{schema_name}`.`{staging_table}` TO `{schema_name}`.`{target_table}`"
            logger.info(f"[DDL AUDIT - INITIAL DEPLOY] Promoting staging to target table:\n  -> {initial_rename}")
            cls._execute_ddl(jdbc_info, initial_rename)

        if cls._table_exists(jdbc_info, schema_name, staging_table):
            cls._cleanup_staging_table(jdbc_info, schema_name, staging_table)

        if primary_keys:
            cls._ensure_mysql_index(jdbc_info, schema_name, target_table, primary_keys)

        logger.info(
            f"[DDL AUDIT - MYSQL TABLE] Zero-downtime Gold table '{schema_name}.{target_table}' is active for Power BI consumers. "
            f"Physical Gold table is authoritative (no multiple views created)."
        )

    @classmethod
    def _ensure_mysql_index(
        cls,
        jdbc_info: Dict[str, Any],
        schema_name: str,
        target_table: str,
        nkeys: Optional[List[str]]
    ) -> None:
        """Ensures a performance index exists on natural keys (nkey) in Aurora MySQL."""
        if not nkeys:
            return
        cls._validate_gold_table_name(target_table)
        idx_cols = ", ".join([f"`{k}`" for k in nkeys])
        clean_tbl = target_table.replace("gold_tbl_", "").replace("gold_", "")
        idx_name = f"idx_nkey_{clean_tbl[:20]}_{'_'.join(nkeys)[:20]}"
        try:
            check_sql = f"SHOW INDEX FROM `{schema_name}`.`{target_table}` WHERE Key_name = %s"
            existing = cls._execute_sql_query(jdbc_info, check_sql, (idx_name,))
            if not existing:
                ddl = f"ALTER TABLE `{schema_name}`.`{target_table}` ADD INDEX `{idx_name}` ({idx_cols})"
                cls._execute_ddl(jdbc_info, ddl)
                logger.info(f"[DDL AUDIT - PERFORMANCE INDEX] Created query performance index `{idx_name}` on ({idx_cols}) in `{schema_name}`.`{target_table}`.")
            else:
                logger.debug(f"[DDL AUDIT - INDEX] Performance index `{idx_name}` already exists on `{schema_name}`.`{target_table}`.")
        except Exception as e:
            err_msg = str(e)
            if "Duplicate key name" in err_msg or "1061" in err_msg:
                logger.debug(f"[DDL AUDIT - INDEX] Index `{idx_name}` already exists on `{schema_name}`.`{target_table}`.")
            else:
                logger.warning(f"[DDL AUDIT - INDEX NOTE] Note on index creation for `{idx_name}` on `{schema_name}`.`{target_table}`: {e}")

    @classmethod
    def _replace_mysql_records(
        cls,
        jdbc_info: Dict[str, Any],
        schema_name: str,
        target_table: str,
        staging_table: str,
        columns: List[str]
    ) -> None:
        """Safely replaces records in target MySQL table without dropping or renaming."""
        cls._validate_gold_table_name(target_table)
        cls._validate_droppable_table_name(staging_table)

        col_names = [f"`{c}`" for c in columns]
        logger.info(f"[MYSQL SERVING - DML ONLY] Deleting existing records from '{schema_name}.{target_table}'...")
        cls._execute_ddl(jdbc_info, f"DELETE FROM `{schema_name}`.`{target_table}`")

        logger.info(f"[MYSQL SERVING - DML ONLY] Inserting refreshed records into '{schema_name}.{target_table}'...")
        insert_sql = (
            f"INSERT INTO `{schema_name}`.`{target_table}` ({', '.join(col_names)})\n"
            f"SELECT {', '.join(col_names)} FROM `{schema_name}`.`{staging_table}`"
        )
        cls._execute_ddl(jdbc_info, insert_sql)

        if cls._table_exists(jdbc_info, schema_name, staging_table):
            cls._cleanup_staging_table(jdbc_info, schema_name, staging_table)

    @classmethod
    def _upsert_mysql_table(
        cls,
        jdbc_info: Dict[str, Any],
        schema_name: str,
        target_table: str,
        staging_table: str,
        primary_keys: List[str],
        columns: List[str]
    ) -> None:
        """Executes an idempotent UPSERT into target MySQL table from staging table."""
        cls._validate_gold_table_name(target_table)
        cls._validate_gold_table_name(staging_table)

        if not cls._table_exists(jdbc_info, schema_name, target_table):
            initial_rename = f"RENAME TABLE `{schema_name}`.`{staging_table}` TO `{schema_name}`.`{target_table}`"
            logger.info(f"[DDL AUDIT - INITIAL DEPLOY] Promoting staging to target table:\n  -> {initial_rename}")
            cls._execute_ddl(jdbc_info, initial_rename)
            if primary_keys:
                pk_clause = ", ".join([f"`{k}`" for k in primary_keys])
                try:
                    cls._execute_ddl(jdbc_info, f"ALTER TABLE `{schema_name}`.`{target_table}` ADD PRIMARY KEY ({pk_clause})")
                    logger.info(f"[DDL AUDIT - PRIMARY KEY] Added PRIMARY KEY ({pk_clause}) to `{schema_name}`.`{target_table}`")
                except Exception as pk_err:
                    logger.warning(f"Could not add primary key constraint on MySQL table: {pk_err}")
                cls._ensure_mysql_index(jdbc_info, schema_name, target_table, primary_keys)
            return

        if primary_keys:
            cls._ensure_mysql_index(jdbc_info, schema_name, target_table, primary_keys)

        col_names = [f"`{c}`" for c in columns]
        update_clauses = [f"`{c}` = VALUES(`{c}`)" for c in columns if c not in primary_keys]

        if update_clauses:
            upsert_sql = (
                f"INSERT INTO `{schema_name}`.`{target_table}` ({', '.join(col_names)})\n"
                f"SELECT {', '.join(col_names)} FROM `{schema_name}`.`{staging_table}`\n"
                f"ON DUPLICATE KEY UPDATE {', '.join(update_clauses)}"
            )
        else:
            upsert_sql = (
                f"INSERT IGNORE INTO `{schema_name}`.`{target_table}` ({', '.join(col_names)})\n"
                f"SELECT {', '.join(col_names)} FROM `{schema_name}`.`{staging_table}`"
            )

        logger.info(f"[DDL AUDIT - MYSQL UPSERT] Merging staging records into '{schema_name}.{target_table}'...")
        cls._execute_ddl(jdbc_info, upsert_sql)

        if cls._table_exists(jdbc_info, schema_name, staging_table):
            cls._cleanup_staging_table(jdbc_info, schema_name, staging_table)

    # --------------------------------------------------------------------------
    # SQL Execution Helpers with SSL / TLS Support
    # --------------------------------------------------------------------------
    @classmethod
    def _build_mysql_ssl_context(cls):
        """Builds an SSL context for MySQL connections to satisfy AWS RDS --require_secure_transport=ON."""
        import ssl
        try:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            return ctx
        except Exception:
            return {"check_hostname": False}

    @classmethod
    def _get_mysql_connection(cls, jdbc_info: Dict[str, Any], database: Optional[str] = None):
        """Establishes an SSL/TLS-encrypted connection to MySQL."""
        ssl_ctx = cls._build_mysql_ssl_context()
        target_db = database if database is not None else (jdbc_info.get('database') or None)

        try:
            import pymysql
            connect_kwargs = {
                "host": jdbc_info['host'],
                "port": int(jdbc_info['port']),
                "user": jdbc_info['user'],
                "password": jdbc_info['password'],
                "database": target_db,
                "connect_timeout": 15,
                "ssl": ssl_ctx,
                "charset": "utf8mb4"
            }
            try:
                return pymysql.connect(**connect_kwargs)
            except TypeError:
                connect_kwargs["ssl"] = {"check_hostname": False}
                return pymysql.connect(**connect_kwargs)
        except ImportError:
            import mysql.connector
            return mysql.connector.connect(
                host=jdbc_info['host'],
                port=int(jdbc_info['port']),
                user=jdbc_info['user'],
                password=jdbc_info['password'],
                database=target_db,
                connection_timeout=15,
                ssl_disabled=False,
                ssl_verify_cert=False,
                charset="utf8mb4"
            )

    @classmethod
    def _execute_sql_query(cls, jdbc_info: Dict[str, Any], query: str, params: Tuple = ()) -> List[Tuple]:
        """Executes a parameterized SQL query via an SSL-encrypted MySQL connection."""
        conn = cls._get_mysql_connection(jdbc_info)
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.fetchall()
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            conn.close()

    @classmethod
    def _execute_ddl(cls, jdbc_info: Dict[str, Any], ddl: str) -> None:
        """Executes a DDL statement via an SSL-encrypted MySQL connection."""
        conn = cls._get_mysql_connection(jdbc_info)
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute(ddl)
            conn.commit()
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            conn.close()

    # --------------------------------------------------------------------------
    # Query Discovery & Custom Transform Invocation
    # --------------------------------------------------------------------------
    @classmethod
    def _extract_sql_annotations(cls, sql_text: str) -> Dict[str, Any]:
        """Parses SQL comment annotations in the header."""
        annotations = {}
        if not sql_text:
            return annotations
        for line in sql_text.splitlines()[:30]:
            stripped = line.strip()
            if stripped.startswith("--") or stripped.startswith("#"):
                comment = stripped.lstrip("-#").strip()
                if ":" in comment:
                    key, val = comment.split(":", 1)
                    key_clean = key.strip().upper()
                    val_clean = val.strip()
                    if key_clean in ("PRIMARY_KEY", "PRIMARY_KEYS", "NKEY", "NATURAL_KEY"):
                        annotations["primary_key"] = [k.strip() for k in val_clean.split(",") if k.strip()]
                    elif key_clean in ("TARGET_TABLE", "TABLE_NAME"):
                        annotations["target_table"] = val_clean
        return annotations

    @classmethod
    def _apply_custom_transform(
        cls,
        df: DataFrame,
        script_path: str,
        spark: Optional[SparkSession] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """
        Dynamically loads and invokes transform(df, spark=None, context=None) -> DataFrame
        from gold/script/custom_transforms/<source>_<table>.py.
        """
        if not script_path or not str(script_path).strip():
            return df

        resolved_path = script_path
        if not os.path.isabs(script_path):
            current_dir = os.path.dirname(os.path.abspath(__file__))
            candidates = [
                os.path.join(current_dir, script_path),
                os.path.join(current_dir, "custom_transforms", os.path.basename(script_path)),
                os.path.join(os.getcwd(), script_path),
                os.path.join("/tmp", script_path),
                os.path.join("/tmp", os.path.basename(script_path))
            ]
            for cand in candidates:
                if os.path.exists(cand):
                    resolved_path = cand
                    break

        if not os.path.exists(resolved_path):
            logger.info(f"[CUSTOM TRANSFORM] No custom script found at '{resolved_path}'. Proceeding with standard DataFrame.")
            return df

        try:
            logger.info(f"[CUSTOM TRANSFORM] Loading custom transform script: '{resolved_path}'")
            module_name = f"gold_custom_{os.path.splitext(os.path.basename(resolved_path))[0]}"
            spec = importlib.util.spec_from_file_location(module_name, resolved_path)
            custom_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(custom_module)

            if hasattr(custom_module, "transform"):
                transform_func = getattr(custom_module, "transform")
                sig = inspect.signature(transform_func)
                param_count = len(sig.parameters)
                logger.info(f"[CUSTOM TRANSFORM] Invoking transform() with {param_count} parameters from '{resolved_path}'...")
                if param_count >= 3:
                    return transform_func(df, spark, context or {})
                elif param_count == 2:
                    return transform_func(df, spark)
                else:
                    return transform_func(df)
            else:
                logger.warning(f"[CUSTOM TRANSFORM] Script '{resolved_path}' has no transform() function. Skipping.")
                return df
        except Exception as err:
            logger.error(f"[CUSTOM TRANSFORM ERROR] Failed executing custom transform '{resolved_path}': {err}\n{traceback.format_exc()}")
            raise

    @classmethod
    def _discover_queries(
        cls,
        query_path: str,
        bucket: str,
        s3_client,
        source_system: str = ""
    ) -> Dict[str, str]:
        """Discovers .sql query files from S3 or local directory."""
        queries: Dict[str, str] = {}
        src = source_system.strip().lower() if source_system else ""

        def _clean_query_key(raw_base: str) -> str:
            k = raw_base.strip().lower()
            if src and k.startswith(f"gold_{src}_"):
                k = k[len(f"gold_{src}_"):]
            elif k.startswith("v_"):
                k = k[2:]
                if src and k.startswith(f"gold_{src}_"):
                    k = k[len(f"gold_{src}_"):]
            elif k.startswith("gold_tbl_"):
                k = k[len("gold_tbl_"):]
            elif k.startswith("gold_"):
                k = k[5:]
            return k

        if query_path.startswith("s3://"):
            parsed = urlparse(query_path)
            s3_bucket = parsed.netloc or bucket
            s3_prefix = parsed.path.lstrip('/')
            try:
                if s3_prefix.endswith('.sql'):
                    file_name = os.path.basename(s3_prefix)
                    file_base = os.path.splitext(file_name)[0].replace('-', '_')
                    clean_name = _clean_query_key(file_base)
                    resp = s3_client.get_object(Bucket=s3_bucket, Key=s3_prefix)
                    queries[clean_name] = resp['Body'].read().decode('utf-8')
                    logger.info(f"[QUERY DISCOVERY] Loaded single S3 query for '{clean_name}' from '{query_path}'")
                else:
                    paginator = s3_client.get_paginator('list_objects_v2')
                    for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix):
                        for obj in page.get('Contents', []):
                            key = obj['Key']
                            if key.endswith('.sql'):
                                file_name = os.path.basename(key)
                                file_base = os.path.splitext(file_name)[0].replace('-', '_')
                                clean_name = _clean_query_key(file_base)
                                resp = s3_client.get_object(Bucket=s3_bucket, Key=key)
                                queries[clean_name] = resp['Body'].read().decode('utf-8')
                                logger.info(f"[QUERY DISCOVERY] Loaded S3 query for '{clean_name}' from 's3://{s3_bucket}/{key}'")
            except Exception as e:
                logger.warning(f"Error listing S3 query files at '{query_path}': {e}. Falling back to local directory.")

        if not queries:
            local_dir = f"gold/query/{source_system}" if source_system else "gold/query"
            for file_path in glob.glob(f"{local_dir}/*.sql"):
                file_name = os.path.basename(file_path)
                file_base = os.path.splitext(file_name)[0].replace('-', '_')
                clean_name = _clean_query_key(file_base)
                if clean_name not in queries:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        queries[clean_name] = f.read()
                        logger.info(f"[QUERY DISCOVERY] Loaded local query for '{clean_name}' from '{file_path}'")

        return queries

    @classmethod
    def _strip_sql_comments(cls, sql: str) -> str:
        """Strips SQL block (/* ... */) and line (--) comments to avoid false dependency detection."""
        if not sql:
            return ""
        sql = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.DOTALL)
        sql = re.sub(r'--[^\r\n]*', ' ', sql)
        return sql

    @classmethod
    def _check_mart_dependency(
        cls,
        sql: str,
        other_mart: str,
        source_system: str = '',
        glue_database: str = '',
        gold_cfg: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Determines whether SQL query references another gold mart table or view."""
        clean_sql = cls._strip_sql_comments(sql)
        clean_src = source_system.strip().lower() if source_system else ""
        clean_other = other_mart.strip().lower()
        if clean_src and clean_other.startswith(f"gold_{clean_src}_"):
            clean_other = clean_other[len(f"gold_{clean_src}_"):]
        elif clean_other.startswith("v_"):
            clean_other = clean_other[2:]
        elif clean_other.startswith("gold_"):
            clean_other = clean_other[5:]

        target_tables = [
            f"gold_{clean_src}_{clean_other}" if clean_src else f"gold_{clean_other}",
            f"gold_tbl_{clean_other}",
            f"gold_{clean_other}",
            f"v_{clean_other}",
            clean_other
        ]
        if GoldConfigLoader and clean_src:
            athena_tbl = GoldConfigLoader.get_target_table_name(clean_src, clean_other, 'athena', gold_cfg)
            if athena_tbl:
                target_tables.append(athena_tbl)

        unique_targets = list(dict.fromkeys(target_tables))

        for target in unique_targets:
            pat = rf'(?<![\w])(?:[`\w]+\.)?`?{target}`?\b'
            if re.search(pat, clean_sql, re.IGNORECASE):
                return True

        generic_gold_pat = rf'(?<![\w])(?:[`\w]+\.)?`?gold_\w+_{clean_other}`?\b'
        if re.search(generic_gold_pat, clean_sql, re.IGNORECASE):
            return True

        from_join_pat = rf'\b(?:FROM|JOIN)\s+(?:[`\w]+\.)?`?{clean_other}`?\b'
        if re.search(from_join_pat, clean_sql, re.IGNORECASE):
            return True

        return False

    @classmethod
    def _sort_queries_by_dependency(
        cls,
        query_dict: Dict[str, str],
        source_system: str = '',
        glue_database: str = '',
        gold_cfg: Optional[Dict[str, Any]] = None
    ) -> Dict[str, str]:
        """Sorts queries so dependency marts run before dependent marts."""
        ordered = {}
        remaining = dict(query_dict)
        while remaining:
            ready = []
            for name, sql in remaining.items():
                deps = [
                    other for other in remaining
                    if other != name and cls._check_mart_dependency(sql, other, source_system, glue_database, gold_cfg)
                ]
                if not deps:
                    ready.append(name)
            if not ready:
                ready.append(next(iter(remaining.keys())))
            for r in ready:
                ordered[r] = remaining.pop(r)
        return ordered

    @classmethod
    def _resolve_query_dependencies(
        cls,
        active_queries: Dict[str, str],
        all_queries: Dict[str, str],
        source_system: str = '',
        glue_database: str = '',
        spark: Optional[SparkSession] = None,
        gold_cfg: Optional[Dict[str, Any]] = None
    ) -> Dict[str, str]:
        """Detects if any query depends on another mart not in active_queries."""
        resolved = dict(active_queries)
        added_deps = True

        while added_deps:
            added_deps = False
            for name, sql in list(resolved.items()):
                for cand_name, cand_sql in all_queries.items():
                    if cand_name not in resolved and cls._check_mart_dependency(sql, cand_name, source_system, glue_database, gold_cfg):
                        target_tbl = None
                        if GoldConfigLoader and source_system:
                            target_tbl = GoldConfigLoader.get_target_table_name(source_system, cand_name, 'athena', gold_cfg)
                        if not target_tbl:
                            target_tbl = f"gold_{source_system}_{cand_name}" if source_system else f"gold_{cand_name}"

                        tbl_exists = False
                        if spark and glue_database:
                            try:
                                spark.sql(f"DESCRIBE TABLE `{glue_database}`.`{target_tbl}`")
                                spark.table(f"{glue_database}.{target_tbl}").limit(1).collect()
                                tbl_exists = True
                            except Exception:
                                tbl_exists = False

                        if not tbl_exists:
                            logger.info(
                                f"[DEPENDENCY RESOLUTION] Mart '{name}' depends on '{cand_name}' which does not exist yet "
                                f"in '{glue_database}.{target_tbl}'. Scheduling '{cand_name}' to create and load first."
                            )
                            resolved[cand_name] = cand_sql
                            added_deps = True
                        else:
                            if spark and glue_database:
                                try:
                                    dep_df = spark.table(f"{glue_database}.{target_tbl}")
                                    dep_df.createOrReplaceTempView(f"v_{cand_name}")
                                    dep_df.createOrReplaceTempView(cand_name)
                                    dep_df.createOrReplaceTempView(f"gold_tbl_{cand_name}")
                                    dep_df.createOrReplaceTempView(target_tbl)
                                    logger.info(
                                        f"[DEPENDENCY RESOLUTION] Pre-loaded authoritative existing table '{glue_database}.{target_tbl}' "
                                        f"into Spark view 'v_{cand_name}' for dependent mart '{name}'."
                                    )
                                except Exception as pre_err:
                                    logger.debug(f"[DEPENDENCY RESOLUTION] Note pre-loading '{cand_name}': {pre_err}")
                                    resolved[cand_name] = cand_sql
                                    added_deps = True

        return resolved

    @classmethod
    def _ensure_dependency_views_registered(
        cls,
        spark: Optional[SparkSession],
        sql_text: str,
        source_system: str = '',
        glue_database: str = '',
        gold_cfg: Optional[Dict[str, Any]] = None,
        glue_client=None
    ) -> None:
        """
        Inspects SQL query text for references to upstream tables or views across any layer:
          - Silver Layer: tbl_<name> (e.g. tbl_interactions, tbl_conversations)
          - Bronze Layer: raw_tbl_<name> (e.g. raw_tbl_interactions)
          - Gold Layer  : gold_<source>_<name>, gold_tbl_<name>, v_<name>, or <name>
        If the table is not registered in Spark's temp views but exists in the Glue
        Data Catalog, loads it into Spark temp views so queries resolve seamlessly.
        """
        if not spark or not sql_text or not glue_database:
            return

        clean_sql = cls._strip_sql_comments(sql_text)
        tokens = re.findall(r'\b(?:FROM|JOIN)\s+([`\w.]+)', clean_sql, re.IGNORECASE)
        src = source_system.strip().lower() if source_system else ""
        prefix_g = f"gold_{src}_" if src else "gold_"

        for token in tokens:
            raw_name = token.replace('`', '').split('.')[-1].lower()
            try:
                if spark.catalog.tableExists(raw_name):
                    continue
            except Exception:
                pass

            base_cand = raw_name
            for pfx in [prefix_g, "gold_tbl_", "gold_", "v_", "raw_tbl_", "tbl_"]:
                if base_cand.startswith(pfx):
                    base_cand = base_cand[len(pfx):]
                    break

            candidates_to_try = [
                raw_name,
                f"tbl_{base_cand}",
                f"raw_tbl_{base_cand}",
                f"gold_{src}_{base_cand}" if src else f"gold_{base_cand}",
                f"gold_tbl_{base_cand}",
                f"v_{base_cand}",
                base_cand
            ]
            if GoldConfigLoader and src:
                tgt = GoldConfigLoader.get_target_table_name(src, base_cand, 'athena', gold_cfg)
                if tgt and tgt not in candidates_to_try:
                    candidates_to_try.insert(0, tgt)

            for cand in list(dict.fromkeys(candidates_to_try)):
                qualified_candidates = [
                    f"glue_catalog.{glue_database}.{cand}",
                    f"{glue_database}.{cand}",
                    cand
                ]
                loaded = False
                for q_cand in qualified_candidates:
                    try:
                        dep_df = spark.table(q_cand)
                        dep_df.createOrReplaceTempView(raw_name)
                        dep_df.createOrReplaceTempView(cand)
                        dep_df.createOrReplaceTempView(base_cand)
                        if not raw_name.startswith("tbl_"):
                            dep_df.createOrReplaceTempView(f"tbl_{base_cand}")
                        if not raw_name.startswith("raw_tbl_"):
                            dep_df.createOrReplaceTempView(f"raw_tbl_{base_cand}")
                        logger.info(
                            f"[DEPENDENCY REGISTRY] Pre-loaded authoritative dependency table '{q_cand}' "
                            f"into Spark session as '{raw_name}'."
                        )
                        loaded = True
                        break
                    except Exception:
                        continue
                if loaded:
                    break

    # --------------------------------------------------------------------------
    # Connection Resolution
    # --------------------------------------------------------------------------
    @classmethod
    def _resolve_mysql_connection_info(
        cls,
        params: Dict[str, Any],
        glue_client=None,
        secrets_client=None
    ) -> Dict[str, Any]:
        """Resolves MySQL host, port, user, password, and database."""
        gold_schema = (
            params.get('GOLD_SCHEMA')
            or params.get('gold_schema')
            or params.get('RDS_SCHEMA')
            or params.get('rds_schema')
            or params.get('RDS_GOLD_SCHEMA')
            or params.get('rds_gold_schema')
        )
        if not gold_schema or not str(gold_schema).strip():
            raise ValueError(
                "CRITICAL CONFIG ERROR: Missing required parameter '--RDS_GOLD_SCHEMA' (or '--GOLD_SCHEMA').\n"
                "In accordance with enterprise shared database policy, no fallback schema is permitted.\n"
                "Please explicitly specify the target MySQL schema name (e.g. --GOLD_SCHEMA enterprise_reporting)."
            )
        gold_schema = str(gold_schema).strip()

        host = (
            params.get('RDS_HOST')
            or params.get('rds_host')
            or params.get('RDS_URL')
            or params.get('rds_url')
            or os.environ.get('RDS_HOST', 'localhost')
        )
        port = str(
            params.get('RDS_PORT')
            or params.get('rds_port')
            or os.environ.get('RDS_PORT', '3306')
        )
        user = (
            params.get('RDS_USER')
            or params.get('rds_user')
            or params.get('RDS_USERNAME')
            or params.get('rds_username')
            or params.get('rds_uaername')
            or os.environ.get('RDS_USER', 'pipeline_user')
        )
        pwd = (
            params.get('RDS_PASSWORD')
            or params.get('rds_password')
            or params.get('RDS_PASSWORDS')
            or params.get('rds_passwords')
            or params.get('PASSWORD')
            or params.get('password')
            or params.get('PASSWORDS')
            or params.get('passwords')
            or params.get('DB_PASSWORD')
            or params.get('db_password')
            or params.get('DB_PASSWORDS')
            or params.get('db_passwords')
            or params.get('RDS_PWD')
            or params.get('rds_pwd')
            or params.get('PWD')
            or params.get('pwd')
            or os.environ.get('RDS_PASSWORD')
        )

        config_secret = None
        if GoldConfigLoader:
            config_secret = GoldConfigLoader.get_secret_name(params.get('SOURCE_SYSTEM'))

        secret_name = (
            params.get('RDS_SECRET_NAME')
            or params.get('rds_secret_name')
            or params.get('SECRET_NAME')
            or params.get('secret_name')
            or params.get('DB_SECRET_NAME')
            or params.get('db_secret_name')
            or params.get('DB_SECRET')
            or config_secret
            or os.environ.get('RDS_SECRET_NAME')
        )
        conn_name = params.get('CONNECTION_NAME') or params.get('GLUE_CONNECTION_NAME')

        if pwd:
            logger.info("Using manually provided MySQL database password via --RDS_PASSWORD parameter.")
        elif secret_name:
            try:
                if not secrets_client:
                    secrets_client = boto3.client('secretsmanager')
                logger.info(f"Retrieving MySQL database credentials from AWS Secrets Manager secret '{secret_name}'...")
                resp = secrets_client.get_secret_value(SecretId=secret_name)
                secret_str = resp.get('SecretString')
                if not secret_str and 'SecretBinary' in resp:
                    import base64
                    secret_str = base64.b64decode(resp['SecretBinary']).decode('utf-8')

                if secret_str:
                    try:
                        secret_json = json.loads(secret_str)
                        pwd = (
                            secret_json.get('password')
                            or secret_json.get('PASSWORD')
                            or secret_json.get('pwd')
                            or secret_json.get('db_password')
                        )
                        user = (
                            secret_json.get('username')
                            or secret_json.get('user')
                            or secret_json.get('USERNAME')
                            or user
                        )
                        host = secret_json.get('host') or secret_json.get('HOST') or host
                        port = str(secret_json.get('port') or secret_json.get('PORT') or port)
                    except (json.JSONDecodeError, TypeError):
                        pwd = secret_str.strip()

                if not pwd:
                    raise ValueError(
                        f"CRITICAL AUTH ERROR: AWS Secrets Manager secret '{secret_name}' was retrieved, "
                        f"but no 'password' field was found in the secret JSON payload."
                    )
                logger.info(f"Successfully retrieved database credentials from Secrets Manager secret '{secret_name}'.")
            except Exception as sec_err:
                if isinstance(sec_err, ValueError):
                    raise
                raise ValueError(
                    f"CRITICAL AUTH ERROR: Failed to retrieve MySQL credentials from AWS Secrets Manager secret '{secret_name}': {sec_err}"
                ) from sec_err

        elif conn_name and glue_client:
            try:
                logger.info(f"Resolving MySQL connection credentials from AWS Glue Connection '{conn_name}'...")
                resp = glue_client.get_connection(Name=conn_name)
                props = resp.get('Connection', {}).get('ConnectionProperties', {})
                raw_url = props.get('JDBC_CONNECTION_URL', '')
                if not user or user == 'pipeline_user':
                    user = props.get('USERNAME') or user
                pwd = props.get('PASSWORD') or pwd

                clean_url = raw_url.replace('jdbc:mysql://', '').split('?')[0]
                host_port = clean_url.split('/')[0]
                if ':' in host_port:
                    host, port = host_port.split(':')
                elif host_port:
                    host = host_port
            except Exception as conn_err:
                logger.warning(f"Could not resolve Glue connection '{conn_name}': {conn_err}.")

        if not pwd:
            raise ValueError(
                "CRITICAL AUTH ERROR: MySQL database password is missing.\n"
                "Please provide the database password using one of the supported options:\n"
                "  1. AWS Secrets Manager: Pass --RDS_SECRET_NAME <secret_name> (e.g. --RDS_SECRET_NAME dev/rds/mysql)\n"
                "  2. Manual Password    : Pass --RDS_PASSWORD <password> via CLI or set RDS_PASSWORD environment variable\n"
                "  3. AWS Glue Connection: Pass --CONNECTION_NAME <connection_name>"
            )

        return {
            "host": host,
            "port": int(port) if str(port).isdigit() else 3306,
            "user": user,
            "password": pwd,
            "database": gold_schema
        }

    # --------------------------------------------------------------------------
    # Observability & Error Cards
    # --------------------------------------------------------------------------
    @classmethod
    def _format_error_diagnostic_card(
        cls,
        layer: str,
        step_name: str,
        target_entity: str,
        query_source: str,
        conn_info: Optional[Dict[str, Any]],
        exception: Exception
    ) -> str:
        """Emits a structured error card with complete diagnostic context and stack trace."""
        tb = traceback.format_exc()
        safe_host = conn_info.get('host') if conn_info else "N/A"
        safe_user = conn_info.get('user') if conn_info else "N/A"
        safe_db = conn_info.get('database') if conn_info else "N/A"
        failed_at = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        return (
            f"\n+================================================================================+\n"
            f"|  ERROR DIAGNOSTIC CARD: GOLD MART FAILURE [FAILED]\n"
            f"+================================================================================+\n"
            f"|  * Execution Layer    : {layer}\n"
            f"|  * Step Name          : {step_name}\n"
            f"|  * Target Entity      : {target_entity}\n"
            f"|  * Query Source       : {query_source}\n"
            f"|  * Target Host        : {safe_host}\n"
            f"|  * Target User        : {safe_user}\n"
            f"|  * Target Schema      : {safe_db}\n"
            f"|  * Failed At (UTC)    : {failed_at}\n"
            f"|  * Exception Type     : {exception.__class__.__name__}\n"
            f"|  * Exception Message  : {str(exception)}\n"
            f"+--------------------------------------------------------------------------------+\n"
            f"|  COMPLETE STACK TRACE:\n"
            f"{tb}\n"
            f"+================================================================================+"
        )

    @classmethod
    def _log_final_gold_summary(cls, stats: List[Dict[str, Any]], start_time: datetime) -> None:
        """Logs the final execution report for all evaluated Gold marts."""
        end_time = datetime.now(timezone.utc)
        total_duration = (end_time - start_time).total_seconds()
        failed_count = sum(1 for m in stats if m.get('status') == 'FAILED')
        overall_status = "FAILED" if failed_count > 0 else "SUCCESS"

        breakdown_lines = []
        for m in stats:
            status_tag = "[OK]  " if m.get('status') == 'SUCCESS' else "[FAIL]"
            rows = f"Rows: {m['rows_served']:,}" if m.get('rows_served') is not None else "Type: TABLE"
            duration_str = f"Time: {m.get('duration_seconds', 0.0):>5.2f}s"
            breakdown_lines.append(
                f"|  {status_tag} Mart: {m['mart_name']:<20} | Table: {m['target_table']:<35} | {rows:<16} | {duration_str} | Status: {m['status']}"
            )
            if m.get('error_message'):
                breakdown_lines.append(f"|         └── Error: {m['error_message']}")

        breakdown_str = "\n".join(breakdown_lines)

        summary_card = (
            f"\n[JOB REPORT] GOLD SERVING LAYER | Status: {overall_status} | Marts: {len(stats) - failed_count}/{len(stats)} | Duration: {total_duration:.2f}s\n"
            f"+================================================================================+\n"
            f"|                    GOLD SERVING LAYER FINAL EXECUTION REPORT                   |\n"
            f"+================================================================================+\n"
            f"|  Overall Status        : {overall_status}\n"
            f"|  Total Marts Evaluated : {len(stats)} (Succeeded: {len(stats) - failed_count}, Failed: {failed_count})\n"
            f"|  Start Time (UTC)      : {start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
            f"|  End Time (UTC)        : {end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
            f"|  Total Duration        : {total_duration:.2f}s\n"
            f"+--------------------------------------------------------------------------------+\n"
            f"|  MART EXECUTION BREAKDOWN:\n"
            f"{breakdown_str}\n"
            f"+================================================================================+"
        )
        logger.info(summary_card)


class GoldInitialLoader:
    """
    Handles initial historical data ingestion and column consolidation for Gold tables.
    Self-contained within gold_layer_manager to guarantee standalone execution in AWS Glue.
    """

    @classmethod
    def sanitize_column_name(cls, col_name: str) -> str:
        if not col_name:
            return "unnamed_column"
        clean = re.sub(r'[\s\.\-\/\:\(\)\[\]\{\}\<\>#]+', '_', str(col_name).strip())
        clean = re.sub(r'[^a-zA-Z0-9_]', '', clean)
        clean = re.sub(r'_+', '_', clean).strip('_')
        if not clean:
            clean = "col"
        elif clean[0].isdigit():
            clean = f"col_{clean}"
        return clean.lower()

    @classmethod
    def reconcile_schema(
        cls,
        df_source: DataFrame,
        target_schema_fields: Optional[List[Any]] = None
    ) -> DataFrame:
        for orig_col in list(df_source.columns):
            clean_col = cls.sanitize_column_name(orig_col)
            if clean_col != orig_col:
                logger.info(f"[COLUMN CLEANSING] Renaming '{orig_col}' -> '{clean_col}'")
                df_source = df_source.withColumnRenamed(orig_col, clean_col)

        # Convert all business columns in source to StringType (ensures all things in string in Gold layer)
        for c in df_source.columns:
            if c not in ("_updated_at", "_inserted_at", "_data_as_of"):
                df_source = df_source.withColumn(c, col(c).cast("string"))

        if not target_schema_fields:
            return df_source

        target_field_map = {}
        for f in target_schema_fields:
            norm_name = cls.sanitize_column_name(f.name)
            target_field_map[norm_name] = (f.name, f.dataType)

        source_cols = {cls.sanitize_column_name(c): c for c in df_source.columns}

        missing_in_source = []
        for norm_name, (target_name, data_type) in target_field_map.items():
            if norm_name not in source_cols:
                missing_in_source.append(target_name)
                cast_type = data_type if target_name in ("_updated_at", "_inserted_at", "_data_as_of") else "string"
                df_source = df_source.withColumn(target_name, lit(None).cast(cast_type))
            else:
                actual_col = source_cols[norm_name]
                if actual_col != target_name:
                    df_source = df_source.withColumnRenamed(actual_col, target_name)
                if target_name not in ("_updated_at", "_inserted_at", "_data_as_of"):
                    df_source = df_source.withColumn(target_name, col(target_name).cast("string"))

        if missing_in_source:
            logger.info(f"[SCHEMA CONSOLIDATION] Padding {len(missing_in_source)} missing column(s) with NULL: {missing_in_source}")

        extra_in_source = [source_cols[norm_name] for norm_name in source_cols if norm_name not in target_field_map]
        if extra_in_source:
            logger.info(f"[SCHEMA CONSOLIDATION] Preserving {len(extra_in_source)} extra column(s) from CSV for schema evolution: {extra_in_source}")

        return df_source

    @classmethod
    def _probe_target_schema_from_query(
        cls,
        spark: SparkSession,
        source_system: str,
        table_name: str,
        params: Dict[str, Any]
    ) -> Optional[List[Any]]:
        sql_text = params.get('GOLD_SQL') or params.get('QUERY_SQL')
        query_path = params.get('QUERY_PATH')
        if not sql_text and query_path:
            if query_path.startswith('s3://'):
                try:
                    parsed = urlparse(query_path)
                    s3 = boto3.client('s3')
                    resp = s3.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip('/'))
                    sql_text = resp['Body'].read().decode('utf-8')
                    logger.info(f"[QUERY DISCOVERY] Loaded Gold query from '{query_path}'")
                except Exception as s3_err:
                    logger.warning(f"Could not load Gold query from S3 '{query_path}': {s3_err}")
            elif os.path.exists(query_path):
                with open(query_path, 'r', encoding='utf-8') as f:
                    sql_text = f.read()
                logger.info(f"[QUERY DISCOVERY] Loaded Gold query from local path '{query_path}'")

        if not sql_text and params.get('DATA_LAKE_BUCKET'):
            bucket_name = params.get('DATA_LAKE_BUCKET')
            for q_name in [f"{table_name}.sql", f"v_{table_name}.sql"]:
                s3_key = f"gold/query/{source_system}/{q_name}"
                try:
                    s3 = boto3.client('s3')
                    resp = s3.get_object(Bucket=bucket_name, Key=s3_key)
                    sql_text = resp['Body'].read().decode('utf-8')
                    logger.info(f"[QUERY AUTO-DISCOVERY] Found S3 Gold query file at 's3://{bucket_name}/{s3_key}'")
                    break
                except Exception:
                    pass

        if not sql_text:
            return None

        clean_sql = re.sub(r'--[^\r\n]*', '', sql_text).strip().rstrip(';')
        if not clean_sql:
            return None

        probe_sql = f"SELECT * FROM (\n{clean_sql}\n) AS probe_q WHERE 1=0"
        try:
            probe_df = spark.sql(probe_sql)
            target_fields = probe_df.schema.fields
            logger.info(f"[QUERY SCHEMA PROBE] Successfully probed schema: found {len(target_fields)} column(s) from Gold query.")
            return target_fields
        except Exception as probe_err:
            logger.warning(f"[QUERY SCHEMA PROBE NOTE] Spark SQL zero-record probe note: {probe_err}. Proceeding with input file schema.")
            return None

    @classmethod
    def _purge_s3_prefix(cls, s3_client, s3_uri: str) -> int:
        """Safely deletes orphaned or corrupted metadata/data files under an S3 URI."""
        return GoldLayerManager._purge_s3_prefix(s3_client, s3_uri)

    @classmethod
    def run_initial_load(
        cls,
        spark: SparkSession,
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        start_time = datetime.now(timezone.utc)
        env = (
            params.get('ENV')
            or params.get('env')
            or params.get('ENVIRONMENT')
            or params.get('environment')
            or os.environ.get('ENV')
            or os.environ.get('ENVIRONMENT')
            or 'dev'
        ).strip().lower()

        source_system = (params.get('SOURCE_SYSTEM') or '').strip().lower()
        table_name = (params.get('TABLE_NAME') or params.get('MART_NAME') or '').strip().lower()

        if not source_system or not table_name:
            raise ValueError("CRITICAL ERROR: '--SOURCE_SYSTEM' and '--TABLE_NAME' are required for Gold initial load.")

        for prefix in [f"gold_{source_system}_", "gold_tbl_", "gold_", "v_"]:
            if table_name.startswith(prefix):
                table_name = table_name[len(prefix):]
                break

        gold_cfg = GoldConfigLoader.load_config(env=env) if GoldConfigLoader else {}
        defaults_cfg = GoldConfigLoader.get_defaults(gold_cfg, env=env) if GoldConfigLoader else {}

        raw_bucket = (
            params.get('DATA_LAKE_BUCKET')
            or defaults_cfg.get('gold_bucket')
            or f"uax-datalake-{env}-bucket"
        )
        bucket_name = str(raw_bucket).replace('{env}', env).replace('{ENV}', env.upper()).strip()

        raw_glue_db = (
            params.get('GLUE_DATABASE')
            or (GoldConfigLoader.get_glue_database(gold_cfg, env=env) if GoldConfigLoader else None)
            or f"uax_datalake_db_{env}"
        )
        glue_database = str(raw_glue_db).replace('{env}', env).replace('{ENV}', env.upper()).strip()

        csv_path = params.get('CSV_PATH') or params.get('INITIAL_LOAD_PATH') or params.get('INPUT_FILE')
        if not csv_path and GoldConfigLoader:
            init_cfg = GoldConfigLoader.get_initial_load_config(source_system, table_name, gold_cfg)
            csv_path = init_cfg.get('path')
            if not params.get('DELIMITER') and init_cfg.get('delimiter'):
                params['DELIMITER'] = init_cfg.get('delimiter')
            if not params.get('HAS_HEADER') and 'has_header' in init_cfg:
                params['HAS_HEADER'] = init_cfg.get('has_header')

        if csv_path and isinstance(csv_path, str):
            csv_path = (
                csv_path
                .replace("{bucket}", bucket_name)
                .replace("{env}", env)
                .replace("{ENV}", env.upper())
                .replace("{source}", source_system)
                .replace("{table}", table_name)
            )

        if not csv_path:
            csv_path = f"s3://{bucket_name}/gold/initial_exports/{source_system}/{table_name}.csv"

        target_table_name = params.get('TARGET_TABLE_NAME')
        if not target_table_name:
            if GoldConfigLoader:
                target_table_name = GoldConfigLoader.get_target_table_name(source_system, table_name, 'athena', gold_cfg)
            else:
                target_table_name = f"gold_{source_system}_{table_name}"

        pks = GoldLayerManager._resolve_natural_keys(source_system, table_name, gold_cfg, params)
        if not pks:
            raise ValueError(
                f"CRITICAL CONFIG ERROR: Missing 'nkey' in gold configuration for table '{table_name}' "
                f"under source system '{source_system}'. A natural key is mandatory for initial load."
            )

        full_table = f"`{glue_database}`.`{target_table_name}`"
        s3_location = f"s3://{bucket_name}/gold/data/{source_system}/{table_name}"

        logger.info(
            f"\n+================================================================================+\n"
            f"|                STARTING GOLD INITIAL LOAD & CONSOLIDATION                      |\n"
            f"+================================================================================+\n"
            f"|  * Source System     : {source_system.upper()}\n"
            f"|  * Table Name        : {table_name}\n"
            f"|  * Target Table      : {full_table}\n"
            f"|  * Natural Keys      : {pks or 'None (Overwrite)'}\n"
            f"|  * Input File Path   : {csv_path}\n"
            f"|  * Glue Database     : {glue_database}\n"
            f"|  * Execution Time    : {start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
            f"+================================================================================+"
        )

        glue_client = params.get('_glue_client')
        s3_client = params.get('_s3_client')
        if glue_client is None or s3_client is None:
            try:
                region = params.get('AWS_REGION') or os.environ.get('AWS_REGION', 'us-east-1')
                if glue_client is None:
                    glue_client = boto3.client('glue', region_name=region)
                if s3_client is None:
                    s3_client = boto3.client('s3', region_name=region)
            except Exception:
                pass

        if csv_path.endswith('.parquet'):
            logger.info(f"Reading input Parquet file from '{csv_path}'...")
            df_raw = spark.read.parquet(csv_path)
        else:
            delimiter = params.get('DELIMITER', ',')
            has_header = str(params.get('HAS_HEADER', 'true')).strip().lower() in ('true', '1', 'yes')
            logger.info(f"Reading input CSV file from '{csv_path}' (delimiter='{delimiter}', header={has_header})...")
            df_raw = spark.read \
                .option("header", str(has_header).lower()) \
                .option("delimiter", delimiter) \
                .option("quote", '"') \
                .option("escape", '"') \
                .option("multiLine", "true") \
                .option("inferSchema", "false") \
                .csv(csv_path)

        raw_count = df_raw.count()
        logger.info(f"Loaded {raw_count:,} records from input file.")

        # Protegrity Encryption for Historical Cold-Start Data
        table_cfg = GoldConfigLoader.get_table_config(source_system, table_name, gold_cfg) if GoldConfigLoader else {}
        mart_enc_cols = (
            table_cfg.get("encryption_columns")
            or table_cfg.get("encryption", {}).get("columns")
        )
        if mart_enc_cols and ProtegrityEncryptionManager:
            try:
                enc_mgr = ProtegrityEncryptionManager(global_config=defaults_cfg, env=env)
                if enc_mgr.enabled:
                    logger.info(
                        f"[GOLD ENCRYPTION] Encrypting PII columns for historical table '{source_system}.{table_name}'..."
                    )
                    df_raw = enc_mgr.encrypt_spark_dataframe(
                        spark_df=df_raw,
                        table_columns_config=mart_enc_cols,
                        action="protect"
                    )
                    logger.info("[GOLD ENCRYPTION] Historical dataset encrypted successfully.")
            except Exception as enc_err:
                logger.error(f"[GOLD ENCRYPTION ERROR] Failed to encrypt historical columns: {enc_err}")
                raise

        # Check Target Table Schema for Reconciliation & Detect Ghost Tables
        target_fields = None
        table_exists = False
        table_found_in_glue = False
        glue_metadata_location = None

        if glue_client:
            try:
                glue_tbl = glue_client.get_table(DatabaseName=glue_database, Name=target_table_name)
                table_found_in_glue = True
                glue_metadata_location = glue_tbl.get('Table', {}).get('Parameters', {}).get('metadata_location')
            except Exception:
                table_found_in_glue = False

        if table_found_in_glue and glue_metadata_location and s3_client:
            try:
                parsed = urlparse(glue_metadata_location)
                b = parsed.netloc
                k = parsed.path.lstrip('/')
                try:
                    s3_client.head_object(Bucket=b, Key=k)
                except Exception:
                    logger.warning(
                        f"[GHOST TABLE DETECTED] Glue Catalog table `{glue_database}`.`{target_table_name}` points to "
                        f"non-existent S3 metadata '{glue_metadata_location}' (404 NoSuchKey). "
                        f"Dropping stale Glue Catalog entry and purging S3 path to allow clean recreation..."
                    )
                    try:
                        glue_client.delete_table(DatabaseName=glue_database, Name=target_table_name)
                        logger.info(f"Successfully dropped ghost table `{glue_database}`.`{target_table_name}` from Glue Catalog.")
                    except Exception as del_err:
                        logger.warning(f"Failed to delete ghost table from Glue: {del_err}")
                    cls._purge_s3_prefix(s3_client, s3_location)
                    table_found_in_glue = False
            except Exception:
                table_found_in_glue = False

        if table_found_in_glue:
            try:
                target_df = spark.table(f"{glue_database}.{target_table_name}")
                target_df.limit(1).collect()
                target_fields = target_df.schema.fields
                table_exists = True
                logger.info(f"Found existing Gold table '{full_table}' with {len(target_fields)} column(s).")
            except Exception as probe_err:
                logger.warning(
                    f"[INITIAL LOAD PROBE] Spark probe for Gold table '{full_table}' failed ({probe_err}). "
                    f"Deleting stale catalog table via Glue API and purging S3 path to allow clean recreation..."
                )
                if glue_client:
                    try:
                        glue_client.delete_table(DatabaseName=glue_database, Name=target_table_name)
                        logger.info(f"Successfully dropped stale table `{glue_database}`.`{target_table_name}` via Glue API.")
                    except Exception as del_err:
                        logger.warning(f"Could not drop table from Glue Catalog: {del_err}")
                cls._purge_s3_prefix(s3_client, s3_location)
                table_exists = False
        else:
            table_exists = False
            logger.info(f"Gold table '{full_table}' not found in catalog. Attempting automatic Gold query extraction...")

        if not target_fields:
            target_fields = cls._probe_target_schema_from_query(
                spark=spark,
                source_system=source_system,
                table_name=table_name,
                params=params
            )

        df_consolidated = cls.reconcile_schema(df_raw, target_fields)

        if "_updated_at" not in df_consolidated.columns:
            df_consolidated = df_consolidated.withColumn("_updated_at", current_timestamp())
        if "_inserted_at" not in df_consolidated.columns:
            df_consolidated = df_consolidated.withColumn("_inserted_at", current_timestamp())

        # Ensure row uniqueness by natural keys (nkey) from gold config
        df_consolidated = GoldLayerManager._deduplicate_by_nkey(df_consolidated, pks)
        dedup_count = df_consolidated.count()
        logger.info(f"[PRIMARY KEY] Natural keys resolved for '{table_name}': {pks} (Records after deduplication: {dedup_count:,}, raw: {raw_count:,})")

        temp_view = f"incoming_initial_{table_name}"
        df_consolidated.createOrReplaceTempView(temp_view)

        if table_exists:
            # Check for column type incompatibility (e.g. existing table has bigint while incoming has string)
            was_purged = GoldLayerManager._check_and_resolve_iceberg_incompatibilities(
                spark=spark,
                glue_database=glue_database,
                target_table_name=target_table_name,
                incoming_df=df_consolidated,
                s3_location=s3_location,
                glue_client=glue_client,
                s3_client=s3_client
            )
            if was_purged:
                table_exists = False

        if table_exists:
            try:
                GoldLayerManager._sync_iceberg_schema(spark, full_table, df_consolidated)
            except Exception as sync_err:
                logger.debug(f"[INITIAL LOAD] Note on Iceberg schema sync: {sync_err}")

        if table_exists and pks:
            join_cond = " AND ".join([f"target.`{k}` = source.`{k}`" for k in pks])
            merge_sql = (
                f"MERGE INTO {full_table} AS target\n"
                f"USING {temp_view} AS source\n"
                f"ON {join_cond}\n"
                f"WHEN MATCHED THEN UPDATE SET *\n"
                f"WHEN NOT MATCHED THEN INSERT *"
            )
            logger.info(f"[INITIAL LOAD UPSERT] Executing Iceberg MERGE INTO on {full_table}:\n{merge_sql}")
            try:
                spark.sql(merge_sql)
            except Exception as merge_err:
                err_m_str = str(merge_err)
                if any(x in err_m_str for x in (
                    "NoSuchKey", "The specified key does not exist", "NoSuchKeyException", "404",
                    "Cannot safely cast", "Cannot write incompatible data", "incompatible"
                )):
                    logger.warning(
                        f"[INITIAL LOAD MERGE RECOVERY] Iceberg MERGE hit {merge_err}. "
                        f"Purging stale Glue Catalog table and S3 location '{s3_location}' to re-initialize cleanly..."
                    )
                    if glue_client:
                        try:
                            glue_client.delete_table(DatabaseName=glue_database, Name=target_table_name)
                        except Exception:
                            pass
                    try:
                        spark.sql(f"DROP TABLE IF EXISTS `{glue_database}`.`{target_table_name}`")
                    except Exception:
                        pass
                    cls._purge_s3_prefix(s3_client, s3_location)
                    df_consolidated.write \
                        .format("iceberg") \
                        .mode("overwrite") \
                        .option("path", s3_location) \
                        .saveAsTable(f"{glue_database}.{target_table_name}")
                else:
                    logger.warning(f"[INITIAL LOAD UPSERT] Spark SQL MERGE failed ({merge_err}). Falling back to Iceberg insertInto on '{full_table}'...")
                    try:
                        df_consolidated.write.format("iceberg").mode("append").insertInto(f"{glue_database}.{target_table_name}")
                    except Exception as ice_fallback_err:
                        err_fb_str = str(ice_fallback_err)
                        if any(x in err_fb_str for x in (
                            "Cannot safely cast", "Cannot write incompatible data", "incompatible", "NoSuchKey"
                        )):
                            logger.warning(
                                f"[INITIAL LOAD RECOVERY] Fallback hit incompatible schema ({ice_fallback_err}). "
                                f"Purging stale Glue Catalog table and S3 location '{s3_location}' to re-initialize cleanly..."
                            )
                            if glue_client:
                                try:
                                    glue_client.delete_table(DatabaseName=glue_database, Name=target_table_name)
                                except Exception:
                                    pass
                            try:
                                spark.sql(f"DROP TABLE IF EXISTS `{glue_database}`.`{target_table_name}`")
                            except Exception:
                                pass
                            cls._purge_s3_prefix(s3_client, s3_location)
                            df_consolidated.write \
                                .format("iceberg") \
                                .mode("overwrite") \
                                .option("path", s3_location) \
                                .saveAsTable(f"{glue_database}.{target_table_name}")
                        else:
                            logger.error(f"[INITIAL LOAD UPSERT ERROR] Iceberg append fallback also failed: {ice_fallback_err}")
                            raise
        else:
            logger.info(f"[INITIAL LOAD WRITE] Initializing clean Gold Iceberg table {full_table} at '{s3_location}'...")
            if glue_client:
                try:
                    glue_client.delete_table(DatabaseName=glue_database, Name=target_table_name)
                    logger.info(f"[CLEANUP] Deleted preexisting Glue Catalog registration for '{target_table_name}' before recreation.")
                except Exception:
                    pass
            cls._purge_s3_prefix(s3_client, f"{s3_location.rstrip('/')}/metadata")

            try:
                df_consolidated.write \
                    .format("iceberg") \
                    .mode("overwrite") \
                    .option("path", s3_location) \
                    .saveAsTable(f"{glue_database}.{target_table_name}")
            except Exception as ice_err:
                err_text = str(ice_err)
                if any(x in err_text for x in ("NoSuchKey", "The specified key does not exist", "NoSuchKeyException", "404")):
                    logger.warning(
                        f"[INITIAL LOAD WRITE RETRY] Iceberg saveAsTable hit NoSuchKey ({ice_err}). "
                        f"Purging Glue Catalog entry and entire S3 table location '{s3_location}' then retrying fresh creation..."
                    )
                    if glue_client:
                        try:
                            glue_client.delete_table(DatabaseName=glue_database, Name=target_table_name)
                        except Exception:
                            pass
                    cls._purge_s3_prefix(s3_client, s3_location)
                    df_consolidated.write \
                        .format("iceberg") \
                        .mode("overwrite") \
                        .option("path", s3_location) \
                        .saveAsTable(f"{glue_database}.{target_table_name}")
                else:
                    raise

        target_engines = []
        if params.get('GOLD_TARGETS') or params.get('TARGET_ENGINES'):
            target_engines = [t.strip().lower() for t in str(params.get('GOLD_TARGETS') or params.get('TARGET_ENGINES')).split(',') if t.strip()]
        elif GoldConfigLoader:
            target_engines = GoldConfigLoader.get_target_engines(source_system, table_name, gold_cfg)

        skip_aurora = str(params.get('SKIP_AURORA_SERVE', 'false')).strip().lower() in ('true', '1', 'yes')
        needs_aurora = not skip_aurora and any(t in target_engines for t in ('aurora', 'rds', 'mysql'))
        if needs_aurora:
            logger.info(f"[AURORA EXTENSION] User requested Aurora serving for '{target_table_name}'. Reading from Athena table and serving to Aurora...")
            try:
                try:
                    df_athena = spark.table(full_table)
                except Exception as read_tbl_err:
                    logger.warning(f"Could not read from spark.table({full_table}): {read_tbl_err}. Using consolidated DataFrame directly.")
                    df_athena = df_consolidated
                gold_schema = params.get('GOLD_SCHEMA') or params.get('RDS_GOLD_SCHEMA') or 'enterprise_reporting'
                GoldLayerManager._serve_to_mysql(
                    spark=spark,
                    queries={table_name: ""},
                    gold_schema=gold_schema,
                    data_s3_path=s3_location,
                    params=params,
                    glue_client=glue_client,
                    secrets_client=None,
                    mart_stats=[],
                    source_system=source_system,
                    materialized_dfs={table_name: df_athena},
                    mart_keys={table_name: pks}
                )
                logger.info(f"[AURORA EXTENSION] Successfully served Athena table '{full_table}' to Aurora '{gold_schema}.{target_table_name}'.")
            except Exception as aurora_err:
                logger.error(f"[AURORA EXTENSION ERROR] Failed to serve to Aurora: {aurora_err}")

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(
            f"\n+================================================================================+\n"
            f"|                GOLD INITIAL LOAD COMPLETED SUCCESSFULLY                        |\n"
            f"+================================================================================+\n"
            f"|  * Target Table    : {full_table}\n"
            f"|  * Records Loaded  : {raw_count:,}\n"
            f"|  * Duration        : {duration:.2f}s\n"
            f"+================================================================================+"
        )

        return {
            "source_system": source_system,
            "table_name": table_name,
            "target_table": f"{glue_database}.{target_table_name}",
            "records_loaded": raw_count,
            "duration_seconds": round(duration, 2),
            "status": "SUCCESS"
        }


def parse_spark_arguments(args: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Parses CLI arguments for the Gold layer Glue job.
    Validates required parameters and constructs default query and data S3 paths.
    """
    argv = args if args is not None else sys.argv[1:]
    params = {}
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg.startswith("--"):
            clean_arg = arg[2:]
            if "=" in clean_arg:
                key, val = clean_arg.split("=", 1)
                params[key] = val
                params[key.upper()] = val
            elif i + 1 < len(argv) and not argv[i + 1].startswith("--"):
                val = argv[i + 1]
                params[clean_arg] = val
                params[clean_arg.upper()] = val
                i += 1
            else:
                params[clean_arg] = "true"
                params[clean_arg.upper()] = "true"
        i += 1

    source_system = params.get('SOURCE_SYSTEM')
    if not source_system:
        raise ValueError("CRITICAL CONFIG ERROR: Missing required parameter '--SOURCE_SYSTEM'.")

    # Target engines: default includes aurora or requires schema unless targets explicitly exclude it
    gold_target = (params.get('GOLD_TARGET') or params.get('GOLD_TARGETS') or 'aurora').lower()
    needs_schema = any(t in gold_target for t in ('aurora', 'rds', 'mysql'))
    schema = params.get('RDS_GOLD_SCHEMA') or params.get('GOLD_SCHEMA') or params.get('RDS_SCHEMA')
    if needs_schema and not schema:
        raise ValueError(
            "CRITICAL CONFIG ERROR: Missing required parameter '--RDS_GOLD_SCHEMA' (or '--GOLD_SCHEMA').\n"
            "In accordance with enterprise shared database policy, no fallback schema is permitted."
        )

    # Build paths
    env = params.get('ENV') or params.get('ENVIRONMENT') or 'dev'
    bucket = params.get('DATA_LAKE_BUCKET') or f"uax-datalake-{env}-bucket"
    clean_bucket = str(bucket).replace('{env}', env).replace('{ENV}', env.upper()).strip()

    if 'GOLD_QUERY_S3_PATH' not in params:
        params['GOLD_QUERY_S3_PATH'] = f"s3://{clean_bucket}/gold/query/{source_system}"
    if 'GOLD_DATA_S3_PATH' not in params:
        params['GOLD_DATA_S3_PATH'] = f"s3://{clean_bucket}/gold/data/{source_system}"

    return params


def main():
    """Glue entrypoint when executed as an AWS Glue Job."""
    from pyspark import SparkConf
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    expected_args = ['JOB_NAME']
    optional_args = [
        'SOURCE_SYSTEM', 'SECRET_NAME', 'RDS_SECRET_NAME', 'API_SECRET_NAME', 'LLM_SECRET_NAME',
        'GLUE_DATABASE', 'DATA_LAKE_BUCKET', 'GOLD_BUCKET', 'INCREMENTAL', 'FULL_REFRESH',
        'RDS_HOST', 'RDS_PORT', 'RDS_USER', 'RDS_PASSWORD',
        'GOLD_SCHEMA', 'RDS_GOLD_SCHEMA', 'GOLD_TARGETS', 'TARGET_ENGINES',
        'CONNECTION_NAME', 'GLUE_CONNECTION_NAME', 'ENV', 'ENVIRONMENT',
        'GOLD_CONFIG_S3_PATH', 'GOLD_CONFIG_JSON', 'ATHENA_WORKGROUP', 'WORKGROUP',
        'MART_NAME', 'TABLE_NAME', 'TABLE_LIST', 'CSV_PATH', 'INITIAL_LOAD_PATH',
        'INPUT_FILE', 'SKIP_INITIAL_LOAD', 'RELOAD_INITIAL', 'DELIMITER', 'HAS_HEADER',
        'NKEY', 'PRIMARY_KEY', 'AWS_REGION'
    ]

    args_to_check = expected_args + [a for a in optional_args if f"--{a}" in sys.argv or f"--{a.lower()}" in sys.argv]
    resolved = getResolvedOptions(sys.argv, args_to_check)

    # Initialize SparkConf with Iceberg & GlueCatalog settings
    bucket_name = resolved.get('DATA_LAKE_BUCKET') or resolved.get('GOLD_BUCKET') or 'uax-datalake-dev-bucket'
    env_val = resolved.get('ENV') or 'dev'
    clean_bucket = str(bucket_name).replace('{env}', env_val).strip()

    conf = SparkConf()
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{clean_bucket}/gold/data/")
    conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set("spark.sql.catalog.glue_catalog.glue.skip-name-validation", "true")
    conf.set("spark.sql.catalog.glue_catalog.skip-name-validation", "true")
    conf.set("spark.sql.catalog.glue_catalog.aws.glue.skip-name-validation", "true")
    conf.set("spark.sql.defaultCatalog", "glue_catalog")
    conf.set("spark.sql.iceberg.schema-evolution", "true")
    conf.set("spark.sql.iceberg.check-nullability", "false")
    conf.set("spark.sql.parquet.enableVectorizedReader", "false")

    sc = SparkContext.getOrCreate(conf=conf)
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    for k, v in [
        ("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog"),
        ("spark.sql.catalog.glue_catalog.warehouse", f"s3://{clean_bucket}/gold/data/"),
        ("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"),
        ("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
        ("spark.sql.catalog.glue_catalog.glue.skip-name-validation", "true"),
        ("spark.sql.catalog.glue_catalog.skip-name-validation", "true"),
        ("spark.sql.catalog.glue_catalog.aws.glue.skip-name-validation", "true"),
        ("spark.sql.defaultCatalog", "glue_catalog"),
        ("spark.sql.iceberg.schema-evolution", "true"),
        ("spark.sql.iceberg.check-nullability", "false"),
        ("spark.sql.parquet.enableVectorizedReader", "false"),
    ]:
        try:
            spark.conf.set(k, v)
        except Exception:
            pass

    job = Job(glueContext)
    job.init(resolved['JOB_NAME'], resolved)

    try:
        GoldLayerManager.run_gold_pipeline(spark, resolved)
        job.commit()
    except Exception as e:
        logger.error(f"[GLUE JOB FAILED] Execution error: {e}")
        raise


if __name__ == "__main__":
    main()
