"""
Gold Layer Configuration Loader.

Loads configuration from local file or S3 path (s3://<bucket>/scripts/gold/config/gold_config.json
or local gold/script/config/gold_config.json).
Modeled uniformly with Bronze and Silver config loaders.
Supports zero CLI parameter passing: automatically resolves config location.
"""

import os
import json
import logging
from typing import Dict, Any, List, Optional, Union

logger = logging.getLogger(__name__)


class GoldConfigLoader:
    """
    Centralized Configuration Loader for Gold Layer Multi-Target Serving Engine.
    """
    _config_cache: Dict[str, Dict[str, Any]] = {}
    _latest_config: Optional[Dict[str, Any]] = None
    _cached_s3_path: Optional[str] = None
    _cached_s3_client: Optional[Any] = None

    @classmethod
    def _clean_table_name(cls, table_name: str, source_system: str) -> str:
        clean = (table_name or '').strip().lower()
        base = clean
        for prefix in [f"gold_{source_system.lower()}_", "gold_tbl_", "gold_", "v_", "tbl_", "raw_tbl_"]:
            if base.startswith(prefix):
                base = base[len(prefix):]
                break
        return base

    @classmethod
    def clear_cache(cls) -> None:
        """Clears the cached configuration."""
        if isinstance(cls._config_cache, dict):
            cls._config_cache.clear()
        else:
            cls._config_cache = {}
        cls._latest_config = None
        cls._cached_s3_path = None
        cls._cached_s3_client = None

    @classmethod
    def set_loaded_config(cls, config_dict: Dict[str, Any], env: Optional[str] = None) -> None:
        """Explicitly registers a loaded config into the cache to guarantee all subsequent calls reuse it."""
        if cls._config_cache is None or not isinstance(cls._config_cache, dict):
            cls._config_cache = {}
        effective_env = (env or os.environ.get('ENV') or os.environ.get('ENVIRONMENT') or 'dev').strip().lower()
        cls._config_cache[effective_env] = config_dict
        cls._latest_config = config_dict

    @classmethod
    def interpolate_env(cls, obj: Any, env: str) -> Any:
        """
        Recursively replaces '{env}' and '{ENV}' placeholders in strings, dictionaries (keys & values), and lists.
        """
        if not env:
            return obj
        env_lower = str(env).strip().lower()
        env_upper = str(env).strip().upper()

        if isinstance(obj, str):
            return obj.replace("{env}", env_lower).replace("{ENV}", env_upper)
        elif isinstance(obj, dict):
            return {
                cls.interpolate_env(k, env): cls.interpolate_env(v, env)
                for k, v in obj.items()
            }
        elif isinstance(obj, list):
            return [cls.interpolate_env(item, env) for item in obj]
        return obj

    @classmethod
    def load_config(
        cls,
        config_s3_path: Optional[str] = None,
        s3_client: Optional[Any] = None,
        env: Optional[str] = None,
        bucket_hint: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Loads the Gold configuration JSON file from S3, CLI arguments, or local disk.
        Dynamically interpolates all {env} placeholders according to effective_env.
        Cached in memory per environment after first load.
        """
        import sys
        import zipfile

        if cls._config_cache is None or not isinstance(cls._config_cache, dict):
            cls._config_cache = {}
        effective_env = (env or os.environ.get('ENV') or os.environ.get('ENVIRONMENT') or 'dev').strip().lower()
        if effective_env in cls._config_cache and cls._config_cache[effective_env]:
            return cls._config_cache[effective_env]

        raw_config = None

        # Tier 0: Direct JSON string passed via CLI parameter or environment variable
        for i, a in enumerate(sys.argv):
            for flag in ('--GOLD_CONFIG_JSON', '--gold_config_json', '--GOLD_CONFIG', '--gold_config'):
                if a == flag and i + 1 < len(sys.argv):
                    val = sys.argv[i + 1].strip()
                    if val.startswith('{') and val.endswith('}'):
                        try:
                            raw_config = json.loads(val)
                            logger.info("[GOLD CONFIG] Loaded Gold configuration directly from CLI raw JSON argument.")
                            break
                        except Exception:
                            pass
                elif a.startswith(f"{flag}="):
                    val = a.split('=', 1)[1].strip()
                    if val.startswith('{') and val.endswith('}'):
                        try:
                            raw_config = json.loads(val)
                            logger.info("[GOLD CONFIG] Loaded Gold configuration directly from CLI raw JSON argument.")
                            break
                        except Exception:
                            pass
            if raw_config is not None:
                break

        if raw_config is None:
            raw_env_json = os.environ.get('GOLD_CONFIG_JSON') or os.environ.get('GOLD_CONFIG')
            if raw_env_json and raw_env_json.strip().startswith('{') and raw_env_json.strip().endswith('}'):
                try:
                    raw_config = json.loads(raw_env_json.strip())
                    logger.info("[GOLD CONFIG] Loaded Gold configuration from GOLD_CONFIG_JSON environment variable.")
                except Exception:
                    pass

        # Tier 1: Explicit CLI / sys.argv path resolution
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

        if config_s3_path and str(config_s3_path).strip():
            cls._cached_s3_path = str(config_s3_path).strip()
        if s3_client is not None:
            cls._cached_s3_client = s3_client

        active_s3_path = config_s3_path or cls._cached_s3_path
        active_s3_client = s3_client or cls._cached_s3_client

        if (not active_s3_client) and (active_s3_path and active_s3_path.startswith("s3://") or bucket_hint):
            try:
                import boto3
                active_s3_client = boto3.client('s3')
                cls._cached_s3_client = active_s3_client
            except Exception as err:
                logger.warning(f"Could not auto-initialize S3 client in GoldConfigLoader: {err}")

        # Tier 2: Try explicit S3 path if provided
        if raw_config is None and active_s3_path and active_s3_path.startswith("s3://") and active_s3_client:
            try:
                path_parts = active_s3_path.replace("s3://", "").split("/", 1)
                bucket_name, object_key = path_parts[0], path_parts[1]
                logger.info(f"[GOLD CONFIG] Attempting to load Gold configuration from explicit S3 path: '{active_s3_path}'")
                response = active_s3_client.get_object(Bucket=bucket_name, Key=object_key)
                content = response['Body'].read().decode('utf-8')
                raw_config = json.loads(content)
                logger.info(f"[GOLD CONFIG] Successfully loaded Gold configuration from explicit S3 path: '{active_s3_path}'")
            except Exception as err:
                logger.warning(f"[GOLD CONFIG] Could not load from '{active_s3_path}': {err}. Probing alternative S3 locations...")

        # Tier 3: Probe candidate S3 paths across buckets
        if raw_config is None and active_s3_client:
            candidate_buckets = []
            if active_s3_path and active_s3_path.startswith("s3://"):
                candidate_buckets.append(active_s3_path.replace("s3://", "").split("/", 1)[0])
            if bucket_hint:
                clean_b = str(bucket_hint).replace('{env}', effective_env).replace('{ENV}', effective_env.upper()).strip()
                if clean_b and clean_b not in candidate_buckets:
                    candidate_buckets.append(clean_b)
            for b_candidate in [
                os.environ.get('DATA_LAKE_BUCKET'),
                os.environ.get('GOLD_BUCKET'),
                os.environ.get('SILVER_BUCKET'),
                f"uax-datalake-{effective_env}-bucket",
                f"uax-datalake-dev-bucket"
            ]:
                if b_candidate:
                    clean_b = str(b_candidate).replace('{env}', effective_env).replace('{ENV}', effective_env.upper()).strip()
                    if clean_b and clean_b not in candidate_buckets:
                        candidate_buckets.append(clean_b)

            relative_keys = [
                "gold/script/config/gold_config.json",
                "gold/config/gold_config.json",
                "scripts/gold/config/gold_config.json",
                "scripts/config/gold_config.json",
                "silver/script/config/gold_config.json",
                "silver/config/gold_config.json",
                "config/gold_config.json",
                "gold_config.json"
            ]

            for b in candidate_buckets:
                if raw_config is not None:
                    break
                for k in relative_keys:
                    candidate_s3_url = f"s3://{b}/{k}"
                    if candidate_s3_url == active_s3_path:
                        continue
                    try:
                        resp = active_s3_client.get_object(Bucket=b, Key=k)
                        raw_config = json.loads(resp['Body'].read().decode('utf-8'))
                        logger.info(f"[GOLD CONFIG] Successfully loaded Gold configuration from probed S3 path: '{candidate_s3_url}'")
                        break
                    except Exception:
                        pass

        # Tier 4: Local filesystem search candidates (handles local repo, Glue /tmp, working directory, and sys.path)
        if raw_config is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            possible_local_paths = [
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

            for path in possible_local_paths:
                if os.path.exists(path) and os.path.isfile(path):
                    try:
                        with open(path, "r", encoding="utf-8") as f:
                            raw_config = json.load(f)
                            logger.info(f"[GOLD CONFIG] Successfully loaded Gold configuration from local file: '{path}'")
                            break
                    except Exception as err:
                        logger.warning(f"Error reading local config file '{path}': {err}")

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

        interpolated = cls.interpolate_env(raw_config, effective_env)
        cls._config_cache[effective_env] = interpolated
        cls._latest_config = interpolated

        sources_detected = list(
            (interpolated.get("source_systems") or interpolated.get("sources") or {}).keys()
        ) if isinstance(interpolated, dict) else []
        if sources_detected:
            logger.info(f"[GOLD CONFIG] Configuration ready for environment '{effective_env}'. Sources: {sources_detected}")

        return interpolated

    @classmethod
    def get_defaults(cls, config_dict: Optional[Dict[str, Any]] = None, env: Optional[str] = None) -> Dict[str, Any]:
        """Retrieves pipeline defaults block."""
        config = config_dict if config_dict is not None else ((cls._config_cache.get(env) if env else cls._latest_config) or cls.load_config(env=env))
        return config.get("pipeline_defaults", {}) if isinstance(config, dict) else {}

    @classmethod
    def get_glue_database(cls, config_dict: Optional[Dict[str, Any]] = None, env: Optional[str] = None) -> str:
        """
        Retrieves glue_database for Gold layer tables from centralized defaults.
        Dynamically interpolates {env} if present.
        """
        effective_env = (env or os.environ.get('ENV') or os.environ.get('ENVIRONMENT') or 'dev').strip().lower()
        defaults = cls.get_defaults(config_dict, env=effective_env)
        db = defaults.get("glue_database") or defaults.get("glue_catalog", {}).get("database_name")
        if not db or not str(db).strip():
            return f"uax_datalake_db_{effective_env}"
        db_str = str(db).strip()
        if "{env}" in db_str or "{ENV}" in db_str:
            db_str = db_str.replace("{env}", effective_env).replace("{ENV}", effective_env.upper())
        return db_str

    @classmethod
    def get_source_config(cls, source_system: str, config_dict: Optional[Dict[str, Any]] = None, env: Optional[str] = None) -> Dict[str, Any]:
        """Retrieves configuration for a specific source system in Gold layer."""
        config = config_dict if config_dict is not None else ((cls._config_cache.get(env) if env else cls._latest_config) or cls.load_config(env=env))
        if not isinstance(config, dict):
            return {}
        sources = config.get("source_systems") or config.get("sources") or {}
        clean_src = (source_system or '').strip().lower()
        if isinstance(sources, dict):
            for k, v in sources.items():
                if k.strip().lower() == clean_src and isinstance(v, dict):
                    return v
        if clean_src in config and isinstance(config[clean_src], dict):
            return config[clean_src]
        return {}

    @classmethod
    def get_table_config(cls, source_system: str, table_name: str, config_dict: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Retrieves table-specific configuration (primary_key, target_engines, transforms, etc.).
        Supports lookup by clean table name, 'v_<name>', or 'gold_<source>_<name>'.
        """
        source_cfg = cls.get_source_config(source_system, config_dict)
        if not isinstance(source_cfg, dict):
            return {}
        table_configs = source_cfg.get("tables") if isinstance(source_cfg.get("tables"), dict) else source_cfg
        if not isinstance(table_configs, dict):
            return {}

        clean = (table_name or '').strip().lower()
        base = clean
        # Strip known prefixes
        for prefix in [f"gold_{source_system.lower()}_", "gold_tbl_", "gold_", "v_", "tbl_", "raw_tbl_"]:
            if base.startswith(prefix):
                base = base[len(prefix):]
                break

        candidates = [clean, base, base.replace('-', '_'), base.replace('_', '-')]
        for cand in candidates:
            for tbl_k, tbl_v in table_configs.items():
                if tbl_k.strip().lower() == cand and isinstance(tbl_v, dict):
                    return dict(tbl_v)

        return {}

    @classmethod
    def get_primary_key(cls, source_system: str, table_name: str, config_dict: Optional[Dict[str, Any]] = None) -> List[str]:
        """
        Retrieves primary key / natural key list for table upsert / merge from gold_config.json.
        Returns empty list if not specified.
        """
        tbl_cfg = cls.get_table_config(source_system, table_name, config_dict)
        for key_name in ("nkey", "primary_key", "natural_key", "pk", "natural_keys", "primary_keys"):
            pk = tbl_cfg.get(key_name)
            if isinstance(pk, list) and pk:
                return [str(k).strip() for k in pk if str(k).strip()]
            elif isinstance(pk, str) and pk.strip():
                return [str(k).strip() for k in pk.split(',') if str(k).strip()]

        return []

    # get_nkey alias for seamless compatibility with Silver layer naming
    get_nkey = get_primary_key

    # get_nkey alias for seamless compatibility with Silver layer naming
    get_nkey = get_primary_key

    @classmethod
    def is_incremental(
        cls,
        source_system: str,
        table_name: str,
        config_dict: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Checks if incremental delta processing is enabled for the table.
        When enabled, only new and modified records (the delta) are passed to custom transforms
        and LLM enrichments, preventing exponential API costs.
        """
        tbl_cfg = cls.get_table_config(source_system, table_name, config_dict)
        if "incremental" in tbl_cfg:
            return bool(tbl_cfg["incremental"])
        defaults = cls.get_defaults(config_dict)
        return bool(defaults.get("incremental", False))

    @classmethod
    def get_llm_column(
        cls,
        source_system: str,
        table_name: str,
        config_dict: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Retrieves configured LLM / enrichment column (e.g. 'sentiment_score', 'llm_summary')
        used to identify unenriched historical records for backfilling.
        """
        tbl_cfg = cls.get_table_config(source_system, table_name, config_dict)
        return tbl_cfg.get("llm_column") or tbl_cfg.get("enrichment_column")

    @classmethod
    def get_secret_name(
        cls,
        source_system: Optional[str] = None,
        config_dict: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Retrieves database secret_name from gold_config.json:
        1. source_systems.<source>.aurora.secret_name
        2. source_systems.<source>.secret_name
        3. pipeline_defaults.aurora.secret_name
        4. pipeline_defaults.secret_name
        """
        cfg = config_dict or cls.load_config()
        if source_system:
            src_cfg = cls.get_source_config(source_system, cfg)
            aurora_cfg = src_cfg.get("aurora", {})
            if isinstance(aurora_cfg, dict) and aurora_cfg.get("secret_name"):
                return aurora_cfg.get("secret_name")
            if src_cfg.get("secret_name"):
                return src_cfg.get("secret_name")

        defaults = cls.get_defaults(cfg)
        aurora_def = defaults.get("aurora", {})
        if isinstance(aurora_def, dict) and aurora_def.get("secret_name"):
            return aurora_def.get("secret_name")
        return defaults.get("secret_name")

    @classmethod
    def get_api_secret_name(
        cls,
        source_system: Optional[str] = None,
        table_name: Optional[str] = None,
        config_dict: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Retrieves external API / LLM secret_name from gold_config.json:
        1. source_systems.<source>.tables.<table>.api_secret_name (or llm_secret_name)
        2. source_systems.<source>.api_secret_name (or llm_secret_name)
        3. pipeline_defaults.api_secret_name (or llm_secret_name)
        """
        cfg = config_dict or cls.load_config()
        if source_system and table_name:
            tbl_cfg = cls.get_table_config(source_system, table_name, cfg)
            if tbl_cfg.get("api_secret_name"):
                return tbl_cfg.get("api_secret_name")
            if tbl_cfg.get("llm_secret_name"):
                return tbl_cfg.get("llm_secret_name")

        if source_system:
            src_cfg = cls.get_source_config(source_system, cfg)
            if src_cfg.get("api_secret_name"):
                return src_cfg.get("api_secret_name")
            if src_cfg.get("llm_secret_name"):
                return src_cfg.get("llm_secret_name")

        defaults = cls.get_defaults(cfg)
        return defaults.get("api_secret_name") or defaults.get("llm_secret_name")

    @classmethod
    def get_target_engines(
        cls,
        source_system: str,
        table_name: str,
        cli_override: Optional[Union[str, List[str]]] = None,
        config_dict: Optional[Dict[str, Any]] = None
    ) -> List[str]:
        """
        Resolves target engines for serving.
        Priority:
        1. CLI override parameter (if passed)
        2. Table-level 'target_engines' in gold_config.json
        3. Source-level 'target_engines' in gold_config.json
        4. Pipeline defaults 'default_target_engines'
        5. Fallback: ['athena']
        """
        if cli_override:
            if isinstance(cli_override, str):
                parsed = [t.strip().lower() for t in cli_override.split(',') if t.strip()]
            else:
                parsed = [t.strip().lower() for t in cli_override if t]
            if parsed:
                return parsed

        tbl_cfg = cls.get_table_config(source_system, table_name, config_dict)
        if tbl_cfg.get("target_engines"):
            return [t.strip().lower() for t in tbl_cfg["target_engines"]]

        source_cfg = cls.get_source_config(source_system, config_dict)
        if source_cfg.get("target_engines"):
            return [t.strip().lower() for t in source_cfg["target_engines"]]

        defaults = cls.get_defaults(config_dict)
        if defaults.get("default_target_engines"):
            return [t.strip().lower() for t in defaults["default_target_engines"]]

        return ["athena"]

    @classmethod
    def get_initial_load_config(
        cls,
        source_system: str,
        table_name: str,
        config_dict: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Resolves initial report/export file configuration from gold_config.json.
        Supports:
          1. tables.<table>.initial_load: {"path": "...", "format": "csv", "delimiter": ",", "has_header": true}
          2. tables.<table>.initial_load_path: "s3://..."
          3. tables.<table>.initial_export_path: "s3://..."
          4. pipeline_defaults.initial_load.template: "s3://{bucket}/gold/initial_exports/{source}/{table}.csv"
        """
        tbl_cfg = cls.get_table_config(source_system, table_name, config_dict)
        init_cfg = tbl_cfg.get("initial_load") or tbl_cfg.get("initial_export") or {}
        if isinstance(init_cfg, str):
            init_cfg = {"path": init_cfg}
        elif isinstance(init_cfg, dict):
            init_cfg = dict(init_cfg)
        else:
            init_cfg = {}

        if not init_cfg.get("path"):
            path = (
                tbl_cfg.get("initial_load_path")
                or tbl_cfg.get("initial_export_path")
                or tbl_cfg.get("historical_export_path")
                or tbl_cfg.get("csv_path")
            )
            if path:
                init_cfg["path"] = path

        defaults = cls.get_defaults(config_dict)
        default_init = defaults.get("initial_load", {})
        if isinstance(default_init, dict):
            if "delimiter" not in init_cfg and "delimiter" in default_init:
                init_cfg["delimiter"] = default_init["delimiter"]
            if "has_header" not in init_cfg and "has_header" in default_init:
                init_cfg["has_header"] = default_init["has_header"]
            if not init_cfg.get("path") and default_init.get("template"):
                init_cfg["path"] = default_init["template"]

        return init_cfg

    @classmethod
    def get_custom_transform_path(
        cls,
        source_system: str,
        table_name: str,
        config_dict: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Resolves the custom transform script path.
        Priority:
        1. Configured custom_transform_script in table config.
        2. Convention-based file: gold/script/custom_transforms/<source>_<table>.py.
        """
        tbl_cfg = cls.get_table_config(source_system, table_name, config_dict)
        configured_path = tbl_cfg.get("custom_transform_script") or tbl_cfg.get("custom_transform_file")
        if configured_path and os.path.exists(configured_path):
            return configured_path

        # Convention-based check
        clean_source = source_system.strip().lower()
        clean_table = table_name.strip().lower()
        for prefix in [f"gold_{clean_source}_", "gold_tbl_", "gold_", "v_"]:
            if clean_table.startswith(prefix):
                clean_table = clean_table[len(prefix):]
                break

        current_dir = os.path.dirname(os.path.abspath(__file__))
        candidate_paths = [
            os.path.join(current_dir, "custom_transforms", f"{clean_source}_{clean_table}.py"),
            f"gold/script/custom_transforms/{clean_source}_{clean_table}.py",
            f"scripts/gold/custom_transforms/{clean_source}_{clean_table}.py",
            f"/tmp/custom_transforms/{clean_source}_{clean_table}.py"
        ]
        for p in candidate_paths:
            if os.path.exists(p):
                return p

        return configured_path

    @classmethod
    def get_target_table_name(
        cls,
        source_system: str,
        table_name: str,
        engine: str = "athena",
        config_dict: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Determines the target table name for the specified engine.
        Standard enterprise convention: gold_<source>_<tablename>
        Identical across Athena, Aurora, Redshift, Snowflake, and Databricks.
        Can be overridden in table config under engine specific section (e.g. aurora.table_name).
        """
        tbl_cfg = cls.get_table_config(source_system, table_name, config_dict)
        clean_source = source_system.strip().lower()
        clean_table = table_name.strip().lower()

        for prefix in [f"gold_{clean_source}_", "gold_tbl_", "gold_", "v_"]:
            if clean_table.startswith(prefix):
                clean_table = clean_table[len(prefix):]
                break

        engine_cfg = tbl_cfg.get(engine.lower(), {})
        if isinstance(engine_cfg, dict) and engine_cfg.get("table_name"):
            return engine_cfg["table_name"]

        if tbl_cfg.get("table_name"):
            return tbl_cfg["table_name"]

        defaults = cls.get_defaults(config_dict)
        prefix = defaults.get("table_prefix", "gold_{source}_")
        if "{source}" in prefix or "{source_system}" in prefix:
            prefix = prefix.replace("{source}", clean_source).replace("{source_system}", clean_source)
        elif prefix == "gold_":
            prefix = f"gold_{clean_source}_"

        return f"{prefix}{clean_table}"
