"""
Gold Layer Initial Load & CSV Reconciliation Engine — AWS Glue / PySpark.

Imports historical exports (CSV/Parquet) into Gold tables with flexible schema consolidation:
1. Reconciles columns:
   - Missing in CSV, present in Gold -> filled with NULL (cast to target type).
   - Present in CSV, missing in Gold -> retained for schema evolution.
2. Performs idempotent UPSERT (MERGE INTO) into Gold table using primary keys.
3. Propagates consolidated records to downstream serving targets (Athena Iceberg table, Aurora MySQL).
"""

import sys
import os
import re
import logging
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Union

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp, date_format,
    when, coalesce, from_unixtime, trim, lower
)

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
    from gold_layer_manager import GoldLayerManager
except ImportError:
    try:
        from gold.script.gold_layer_manager import GoldLayerManager
    except ImportError:
        GoldLayerManager = None

try:
    from protegrity_encryption import ProtegrityEncryptionManager
except ImportError:
    try:
        from gold.script.protegrity_encryption import ProtegrityEncryptionManager
    except ImportError:
        ProtegrityEncryptionManager = None

logger = logging.getLogger(__name__)


class GoldInitialLoader:
    """
    Handles initial historical data ingestion and column consolidation for Gold tables.
    """

    @classmethod
    def sanitize_column_name(cls, col_name: str) -> str:
        """
        Cleanses and standardizes column names for SQL, Parquet, Iceberg, and Athena compatibility:
        - Converts spaces (' ') and periods ('.') into underscores ('_').
        - Converts hyphens ('-'), slashes ('/'), colons (':'), brackets into underscores ('_').
        - Strips illegal/non-alphanumeric characters.
        - Collapses duplicate underscores and converts to lowercase snake_case.
        """
        if not col_name:
            return "unnamed_column"
        # 1. Replace spaces, dots, hyphens, slashes, brackets, colons, hashes with underscore
        clean = re.sub(r'[\s\.\-\/\:\(\)\[\]\{\}\<\>#]+', '_', str(col_name).strip())
        # 2. Strip non-alphanumeric characters except underscore
        clean = re.sub(r'[^a-zA-Z0-9_]', '', clean)
        # 3. Collapse multiple underscores and trim leading/trailing underscores
        clean = re.sub(r'_+', '_', clean).strip('_')
        # 4. Handle leading digits or empty
        if not clean:
            clean = "col"
        elif clean[0].isdigit():
            clean = f"col_{clean}"
        return clean.lower()

    @classmethod
    def _is_timestamp_column(
        cls,
        col_name: str,
        source_type: Optional[Any] = None,
        target_type: Optional[Any] = None
    ) -> bool:
        """
        Determines whether a column represents a date/time/timestamp:
        1. If source_type or target_type is TimestampType/DateType/TimestampNTZType.
        2. If column name matches common timestamp conventions (exact, prefix, suffix, phrase).
        Excludes technical audit metadata columns (_updated_at, _inserted_at, _data_as_of).
        """
        if not col_name:
            return False

        c_clean = str(col_name).strip()
        if c_clean in ("_updated_at", "_inserted_at", "_data_as_of"):
            return False

        # 1. Inspect PySpark data types if available
        for dt in (source_type, target_type):
            if dt is not None:
                type_name = str(dt).lower()
                if any(k in type_name for k in ("timestamptype", "datetype", "timestamp_ntz")) and not any(k in type_name for k in ("string", "varchar")):
                    return True
                if hasattr(dt, "typeName"):
                    try:
                        tn = str(dt.typeName()).lower()
                        if tn in ("timestamp", "date", "timestamp_ntz"):
                            return True
                    except Exception:
                        pass

        # 2. Inspect Column Name Naming Conventions (normalized lowercase)
        c_norm = cls.sanitize_column_name(c_clean).lower()

        # Exact matches
        if c_norm in (
            "timestamp", "ts", "datetime", "date", "time",
            "start_time", "end_time", "created_time", "updated_time",
            "start_date", "end_date", "conversation_start", "conversation_end",
            "created_at", "updated_at", "closed_at", "resolved_at"
        ):
            return True

        # Suffixes
        if c_norm.endswith((
            "_timestamp", "_ts", "_datetime", "_at", "_time",
            "_date", "_dt", "_on", "_start", "_end"
        )):
            return True

        # Prefixes
        if c_norm.startswith(("timestamp_", "datetime_", "date_", "time_")):
            return True

        # Specific compound phrases
        if any(p in c_norm for p in (
            "_timestamp_", "_datetime_", "_date_", "_time_",
            "conversation_start", "conversation_end", "start_time", "end_time",
            "created_at", "updated_at", "sys_created", "sys_updated",
            "event_time", "event_timestamp"
        )):
            return True

        return False

    @classmethod
    def _conform_timestamp_to_string(
        cls,
        df: DataFrame,
        col_name: str,
        data_type: Optional[Any] = None
    ) -> DataFrame:
        """
        Converts an identified timestamp column to 'YYYY-MM-DD HH:MM:SS' format
        and ensures its final type is StringType.
        Handles:
        - Native TimestampType, DateType, TimestampNTZType
        - Numeric epochs (seconds: 10 digits, milliseconds: 13 digits)
        - ISO-8601 strings ('2024-09-24T11:00:00Z', '2024-09-24T11:00:00.000Z', '2024-09-24T11:00:00+00:00')
        - Standard space-separated strings ('2024-09-24 11:00:00', '2024-09-24 11:00:00.123')
        - Slash strings ('2024/09/24 11:00:00', '09/24/2024 11:00:00')
        - Date-only strings ('2024-09-24', '2024/09/24', '09/24/2024')
        - Null / empty / blank strings -> NULL (cast to string)
        """
        type_str = str(data_type).lower() if data_type is not None else ""

        # Case 1: Native TimestampType / DateType / TimestampNTZType
        if any(t in type_str for t in ("timestamptype", "datetype", "timestamp_ntz", "timestamp", "date")) and not any(t in type_str for t in ("string", "varchar")):
            return df.withColumn(
                col_name,
                when(col(col_name).isNull(), lit(None).cast("string"))
                .otherwise(date_format(col(col_name), "yyyy-MM-dd HH:mm:ss").cast("string"))
            )

        # Case 2: Numeric Epoch Type (LongType, IntegerType, DoubleType, DecimalType)
        if any(t in type_str for t in ("longtype", "integertype", "shorttype", "doubletype", "floattype", "decimaltype", "bigint", "int", "integer", "double", "float")):
            col_d = col(col_name).cast("double")
            ts_from_epoch = when(
                (col_d >= 1e11) & (col_d <= 3e12),
                to_timestamp(from_unixtime(col_d / 1000.0))
            ).when(
                (col_d >= 1e8) & (col_d <= 3e10),
                to_timestamp(from_unixtime(col_d))
            )
            return df.withColumn(
                col_name,
                when(col(col_name).isNull(), lit(None).cast("string"))
                .when(ts_from_epoch.isNotNull(), date_format(ts_from_epoch, "yyyy-MM-dd HH:mm:ss").cast("string"))
                .otherwise(col(col_name).cast("string"))
            )

        # Case 3: String Type (or un-inferred CSV column)
        col_str = trim(col(col_name).cast("string"))
        is_empty_or_null = (
            col(col_name).isNull()
            | (col_str == "")
            | lower(col_str).isin("null", "none", "nan", "undefined")
        )

        parsed_ts = coalesce(
            to_timestamp(col_str),
            to_timestamp(col_str, "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
            to_timestamp(col_str, "yyyy-MM-dd'T'HH:mm:ssX"),
            to_timestamp(col_str, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
            to_timestamp(col_str, "yyyy-MM-dd'T'HH:mm:ss"),
            to_timestamp(col_str, "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col_str, "yyyy-MM-dd HH:mm:ss.SSSSSS"),
            to_timestamp(col_str, "yyyy-MM-dd HH:mm:ss.SSS"),
            to_timestamp(col_str, "yyyy-MM-dd"),
            to_timestamp(col_str, "yyyy/MM/dd HH:mm:ss"),
            to_timestamp(col_str, "yyyy/MM/dd"),
            to_timestamp(col_str, "MM/dd/yyyy HH:mm:ss"),
            to_timestamp(col_str, "MM/dd/yyyy"),
            to_timestamp(col_str, "dd/MM/yyyy HH:mm:ss"),
            to_timestamp(col_str, "dd/MM/yyyy"),
            # Epoch millis (13 digits)
            when(col_str.rlike(r"^\d{13}$"), to_timestamp(from_unixtime(col_str.cast("double") / 1000.0))),
            # Epoch seconds (10 digits)
            when(col_str.rlike(r"^\d{10}$"), to_timestamp(from_unixtime(col_str.cast("double"))))
        )

        return df.withColumn(
            col_name,
            when(is_empty_or_null, lit(None).cast("string"))
            .when(parsed_ts.isNotNull(), date_format(parsed_ts, "yyyy-MM-dd HH:mm:ss").cast("string"))
            .otherwise(col_str.cast("string"))
        )

    @classmethod
    def reconcile_schema(
        cls,
        df_source: DataFrame,
        target_schema_fields: Optional[List[Any]] = None
    ) -> DataFrame:
        """
        Consolidates source DataFrame columns against target table schema fields:
        1. Cleanses and sanitizes all source column names (converting ' ' and '.' to '_').
        2. Identifies timestamp columns and standardizes them to 'YYYY-MM-DD HH:MM:SS' string format.
        3. Converts all other business columns to StringType to enforce uniform string schema in Gold layer.
        4. Matches source columns against target schema fields (case-insensitively).
        5. Missing in source -> padded with NULL (cast to target type or StringType).
        6. Extra in source -> retained for schema evolution as StringType.
        Returns the cleansed and reconciled DataFrame with uniform string types.
        """
        # 1. Cleanse source column names (spaces -> _, dots -> _, hyphens -> _)
        for orig_col in list(df_source.columns):
            clean_col = cls.sanitize_column_name(orig_col)
            if clean_col != orig_col:
                logger.info(f"[COLUMN CLEANSING] Renaming '{orig_col}' -> '{clean_col}'")
                df_source = df_source.withColumnRenamed(orig_col, clean_col)

        # Build target field map early if available: normalized_name -> (canonical_target_name, dataType)
        target_field_map = {}
        if target_schema_fields:
            for f in target_schema_fields:
                norm_name = cls.sanitize_column_name(f.name)
                target_field_map[norm_name] = (f.name, f.dataType)

        # 2. Identify timestamp columns and standardize to 'YYYY-MM-DD HH:MM:SS', then cast to StringType
        for c in df_source.columns:
            if c in ("_updated_at", "_inserted_at", "_data_as_of"):
                continue
            norm_c = cls.sanitize_column_name(c)
            target_data_type = target_field_map[norm_c][1] if norm_c in target_field_map else None
            source_data_type = None
            try:
                source_data_type = df_source.schema[c].dataType
            except Exception:
                pass

            effective_data_type = target_data_type or source_data_type
            if target_data_type and cls._is_timestamp_column(c, None, target_data_type):
                effective_data_type = target_data_type
            elif source_data_type and cls._is_timestamp_column(c, source_data_type, None):
                effective_data_type = source_data_type

            if cls._is_timestamp_column(c, source_data_type, target_data_type):
                logger.info(f"[TIMESTAMP STANDARDIZATION] Standardizing timestamp column '{c}' to 'YYYY-MM-DD HH:MM:SS' string format.")
                df_source = cls._conform_timestamp_to_string(df_source, c, effective_data_type)
            else:
                df_source = df_source.withColumn(c, col(c).cast("string"))

        if not target_schema_fields:
            return df_source

        source_cols = {cls.sanitize_column_name(c): c for c in df_source.columns}

        # 3. Pad missing columns with NULL cast to string (or target type for audit timestamps)
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

        if missing_in_source:
            logger.info(f"[SCHEMA CONSOLIDATION] Padding {len(missing_in_source)} missing column(s) with NULL: {missing_in_source}")

        # 4. Retain extra columns from source for schema evolution (already cast to string)
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
        """
        Automatically extracts the target schema from the Gold SQL select query:
        1. Checks custom query parameter override (--QUERY_PATH or --GOLD_SQL).
        2. Automatically discovers local or S3 SQL files (e.g. gold/query/<source>/<table>.sql).
        3. Executes a zero-record probe query (SELECT * FROM (<sql>) WHERE 1=0) via Spark SQL.
        4. Extracts schema fields and data types in milliseconds without reading data.
        """
        # 1. Direct SQL override via params
        sql_text = params.get('GOLD_SQL') or params.get('QUERY_SQL')

        # 2. Query file path override
        query_path = params.get('QUERY_PATH')
        if not sql_text and query_path:
            if query_path.startswith('s3://'):
                try:
                    import boto3
                    from urllib.parse import urlparse
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

        clean_tbl = table_name.strip().lower()
        src = source_system.strip().lower()
        for p in (f"gold_{src}_", "gold_tbl_", "gold_", "v_"):
            while clean_tbl.startswith(p):
                clean_tbl = clean_tbl[len(p):]

        # 3. Automatic Discovery in local repository
        if not sql_text:
            candidate_local_paths = [
                os.path.join(script_dir, "..", "query", source_system, f"{clean_tbl}.sql"),
                os.path.join(script_dir, "..", "query", source_system, f"v_{clean_tbl}.sql"),
                os.path.join(script_dir, "..", "query", source_system, f"gold_{src}_{clean_tbl}.sql"),
                os.path.join(script_dir, "..", "query", f"{clean_tbl}.sql"),
                os.path.join("gold", "query", source_system, f"{clean_tbl}.sql"),
                os.path.join("gold", "query", source_system, f"v_{clean_tbl}.sql"),
                os.path.join("gold", "query", source_system, f"gold_{src}_{clean_tbl}.sql"),
            ]
            for c_path in candidate_local_paths:
                norm_c = os.path.abspath(c_path)
                if os.path.exists(norm_c):
                    try:
                        with open(norm_c, 'r', encoding='utf-8') as f:
                            sql_text = f.read()
                        logger.info(f"[QUERY AUTO-DISCOVERY] Found local Gold query file at '{norm_c}'")
                        break
                    except Exception as f_err:
                        logger.debug(f"Error reading candidate query path '{norm_c}': {f_err}")

        # 4. S3 discovery under default convention: s3://<bucket>/gold/query/<source>/<table>.sql
        if not sql_text and params.get('DATA_LAKE_BUCKET'):
            bucket_name = params.get('DATA_LAKE_BUCKET')
            for q_name in [f"{clean_tbl}.sql", f"v_{clean_tbl}.sql", f"gold_{src}_{clean_tbl}.sql", f"{table_name}.sql"]:
                s3_key = f"gold/query/{source_system}/{q_name}"
                try:
                    import boto3
                    s3 = boto3.client('s3')
                    resp = s3.get_object(Bucket=bucket_name, Key=s3_key)
                    sql_text = resp['Body'].read().decode('utf-8')
                    logger.info(f"[QUERY AUTO-DISCOVERY] Found S3 Gold query file at 's3://{bucket_name}/{s3_key}'")
                    break
                except Exception:
                    pass

        if not sql_text or not isinstance(sql_text, str):
            logger.info(f"[QUERY AUTO-DISCOVERY] No query found for '{source_system}.{table_name}'. Initial load will use input file schema.")
            return None

        # Clean SQL comments and annotations
        clean_sql = re.sub(r'--[^\r\n]*', '', sql_text).strip().rstrip(';')
        if not clean_sql:
            return None

        # Pre-register any referenced upstream dependency views
        if GoldLayerManager:
            try:
                g_db = params.get('GLUE_DATABASE') or params.get('DATABASE_NAME', '')
                g_cfg = params.get('GOLD_CONFIG') or (GoldConfigLoader.load_config(env=params.get('ENV', 'dev')) if GoldConfigLoader else None)
                GoldLayerManager._ensure_dependency_views_registered(
                    spark=spark,
                    sql_text=clean_sql,
                    source_system=source_system,
                    glue_database=g_db,
                    gold_cfg=g_cfg
                )
            except Exception as dep_err:
                logger.debug(f"[QUERY SCHEMA PROBE] Dependency view registration note: {dep_err}")

        # Execute ultra-fast zero-data probe query
        probe_sql = f"SELECT * FROM (\n{clean_sql}\n) AS probe_q WHERE 1=0"
        logger.info(f"[QUERY SCHEMA PROBE] Probing target schema from Gold query via zero-record probe (WHERE 1=0)...")
        try:
            probe_df = spark.sql(probe_sql)
            target_fields = probe_df.schema.fields
            logger.info(f"[QUERY SCHEMA PROBE] Successfully probed schema: found {len(target_fields)} column(s) from Gold query.")
            return target_fields
        except Exception as probe_err:
            logger.warning(
                f"[QUERY SCHEMA PROBE NOTE] Spark SQL zero-record probe note: {probe_err}. "
                f"(Underlying Silver tables may not be registered in session; proceeding with input file schema)."
            )
            return None

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
            return df
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
            except Exception:
                pass
        return df.dropDuplicates(subset=valid_nkeys)

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

        import importlib.util
        import inspect

        resolved_path = None
        if os.path.isabs(script_path) and os.path.exists(script_path):
            resolved_path = script_path
        elif str(script_path).startswith("s3://"):
            try:
                import boto3
                match = re.match(r"^s3://([^/]+)/(.*)$", script_path)
                if match:
                    s3_bucket, s3_key = match.groups()
                    local_dir = "/tmp/custom_transforms"
                    os.makedirs(local_dir, exist_ok=True)
                    local_file = os.path.join(local_dir, os.path.basename(s3_key))
                    s3 = boto3.client("s3")
                    s3.download_file(s3_bucket, s3_key, local_file)
                    if os.path.exists(local_file):
                        resolved_path = local_file
            except Exception as s3_err:
                logger.warning(f"[CUSTOM TRANSFORM] S3 download error for '{script_path}': {s3_err}")
        else:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            workspace_root = os.getcwd()

            ctx = context or {}
            src_name = str(ctx.get("source_system", "")).strip().lower()
            tbl_name = str(ctx.get("table_name", "")).strip().lower()
            for prefix in [f"gold_{src_name}_", f"{src_name}_", "gold_"]:
                if tbl_name.startswith(prefix):
                    tbl_name = tbl_name[len(prefix):]
                    break

            base_file = os.path.basename(script_path)
            base_variants = [base_file]
            if src_name and tbl_name:
                convention_name = f"{src_name}_{tbl_name}.py"
                if convention_name not in base_variants:
                    base_variants.append(convention_name)

            # Generate relative path variants (e.g. custom_transform/ <-> custom_transforms/)
            rel_variants = [script_path]
            if "custom_transform/" in script_path and "custom_transforms/" not in script_path:
                rel_variants.append(script_path.replace("custom_transform/", "custom_transforms/"))
            elif "custom_transforms/" in script_path:
                rel_variants.append(script_path.replace("custom_transforms/", "custom_transform/"))
            if src_name and tbl_name:
                rel_variants.append(f"gold/script/custom_transforms/{src_name}_{tbl_name}.py")
                rel_variants.append(f"custom_transforms/{src_name}_{tbl_name}.py")

            search_dirs = [
                current_dir,
                os.path.join(current_dir, "custom_transforms"),
                os.path.join(current_dir, "custom_transform"),
                workspace_root,
                os.path.join(workspace_root, "gold", "script", "custom_transforms"),
                os.path.join(workspace_root, "gold", "script", "custom_transform"),
                "/tmp/custom_transforms",
                "/tmp"
            ]

            for d in search_dirs:
                for r in rel_variants:
                    cand = os.path.normpath(os.path.join(d, r))
                    if os.path.isfile(cand):
                        resolved_path = cand
                        break
                if resolved_path:
                    break

            if not resolved_path:
                for d in search_dirs:
                    for b in base_variants:
                        cand = os.path.normpath(os.path.join(d, b))
                        if os.path.isfile(cand):
                            resolved_path = cand
                            break
                    if resolved_path:
                        break

            # 1. Fallback to Python module import via sys.path / extra-py-files
            if not resolved_path:
                mod_candidates = []
                for b in base_variants:
                    mod_root = os.path.splitext(b)[0]
                    mod_candidates.extend([
                        mod_root,
                        f"custom_transforms.{mod_root}",
                        f"gold.script.custom_transforms.{mod_root}"
                    ])
                for m_cand in mod_candidates:
                    try:
                        spec = importlib.util.find_spec(m_cand)
                        if spec and spec.origin and os.path.isfile(spec.origin):
                            resolved_path = spec.origin
                            break
                    except Exception:
                        pass

            # 2. Fallback to Amazon S3 download (when executing on AWS Glue / EMR worker)
            s3_bucket = None
            if not resolved_path:
                params_dict = ctx.get("params", {}) or {}
                s3_bucket = (
                    ctx.get("bucket_name")
                    or params_dict.get("DATA_LAKE_BUCKET")
                    or params_dict.get("data_lake_bucket")
                    or params_dict.get("GOLD_BUCKET")
                    or params_dict.get("gold_bucket")
                    or params_dict.get("BUCKET_NAME")
                    or params_dict.get("bucket_name")
                )
                if not s3_bucket and params_dict.get("GOLD_CONFIG_S3_PATH"):
                    cfg_s3 = str(params_dict.get("GOLD_CONFIG_S3_PATH"))
                    m = re.match(r"^s3://([^/]+)/", cfg_s3)
                    if m:
                        s3_bucket = m.group(1)

                if not s3_bucket and GoldConfigLoader:
                    try:
                        gold_cfg = GoldConfigLoader.load_config()
                        defaults = GoldConfigLoader.get_defaults(gold_cfg)
                        s3_bucket = defaults.get("gold_bucket")
                    except Exception:
                        pass

                if s3_bucket:
                    s3_keys_to_try = []
                    for r in rel_variants:
                        clean_r = r.lstrip("/")
                        if clean_r not in s3_keys_to_try:
                            s3_keys_to_try.append(clean_r)
                        if not clean_r.startswith("gold/script/"):
                            s3_keys_to_try.append(f"gold/script/{clean_r}")
                        if not clean_r.startswith("scripts/gold/"):
                            s3_keys_to_try.append(f"scripts/gold/{clean_r}")

                    for b in base_variants:
                        for s_prefix in [
                            "gold/script/custom_transforms/",
                            "gold/script/custom_transform/",
                            "scripts/gold/custom_transforms/",
                            "scripts/gold/custom_transform/",
                            "custom_transforms/",
                            "custom_transform/"
                        ]:
                            k = f"{s_prefix}{b}"
                            if k not in s3_keys_to_try:
                                s3_keys_to_try.append(k)

                    s3_client_obj = ctx.get("_s3_client")
                    if not s3_client_obj:
                        try:
                            import boto3
                            s3_client_obj = boto3.client("s3")
                        except Exception:
                            s3_client_obj = None

                    if s3_client_obj:
                        local_dir = "/tmp/custom_transforms"
                        os.makedirs(local_dir, exist_ok=True)
                        for s3_key in s3_keys_to_try:
                            try:
                                s3_client_obj.head_object(Bucket=s3_bucket, Key=s3_key)
                                local_dest = os.path.join(local_dir, os.path.basename(s3_key))
                                logger.info(f"[CUSTOM TRANSFORM] Found script on S3 at 's3://{s3_bucket}/{s3_key}'. Downloading to '{local_dest}'...")
                                s3_client_obj.download_file(s3_bucket, s3_key, local_dest)
                                if os.path.isfile(local_dest):
                                    resolved_path = local_dest
                                    break
                            except Exception:
                                continue

        if not resolved_path or not os.path.exists(resolved_path):
            s3_checked_note = f" (checked local search dirs and s3://{s3_bucket}/)" if s3_bucket else ""
            logger.info(f"[CUSTOM TRANSFORM] No custom script found at '{script_path}'{s3_checked_note}. Proceeding with standard DataFrame.")
            return df

        ctx = context or {}
        source_system = ctx.get("source_system", "")
        table_name = ctx.get("table_name", "table")
        is_initial_load = ctx.get("is_initial_load", False)
        external_cols = ctx.get("external_columns", [])

        logger.info(
            f"[CUSTOM TRANSFORM] Calling custom script for '{source_system}.{table_name}': '{resolved_path}' "
            f"(is_initial_load={is_initial_load}, external_columns={external_cols})..."
        )

        try:
            import importlib.util
            import inspect
            import traceback
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
            logger.error(f"[CUSTOM TRANSFORM ERROR] Failed executing custom transform '{resolved_path}': {err}")
            raise

    @classmethod
    def _resolve_and_deduplicate_columns(
        cls,
        df: DataFrame,
        primary_keys: Optional[List[str]] = None,
        external_columns: Optional[Union[List[str], str]] = None,
        llm_column: Optional[str] = None
    ) -> DataFrame:
        """
        Enforces strict Column Conflict Resolution & Deduplication:
        Precedence Hierarchy:
        Custom Transform (df.withColumn) > SQL Query Column > Config Default (NULL)

        1. Ensures no duplicate or ambiguous column references exist case-insensitively.
        2. Retains the highest-priority column instance (Custom Transform overrides SQL and Config).
        3. Anchors primary keys in deterministic positions.
        4. Injects Config Default (NULL) for any configured external_columns missing from DataFrame.
        """
        if df is None:
            return df

        pks = [k.strip() for k in (primary_keys or [])]
        seen_case_insensitive = set()
        deduped_columns = []

        # 1. Anchor natural keys (pks) first if present
        for pk in pks:
            pk_lower = pk.lower()
            if pk in df.columns and pk_lower not in seen_case_insensitive:
                seen_case_insensitive.add(pk_lower)
                deduped_columns.append(pk)

        # 2. Iterate through DataFrame columns in order (Transform > SQL)
        for c in df.columns:
            c_lower = c.strip().lower()
            if c_lower not in seen_case_insensitive:
                seen_case_insensitive.add(c_lower)
                deduped_columns.append(c)

        # 3. Resolve list of external columns
        target_ext_cols = []
        if external_columns:
            if isinstance(external_columns, list):
                target_ext_cols.extend([str(c).strip() for c in external_columns if str(c).strip()])
            elif isinstance(external_columns, str) and external_columns.strip():
                target_ext_cols.append(external_columns.strip())
        if llm_column and str(llm_column).strip():
            if str(llm_column).strip() not in target_ext_cols:
                target_ext_cols.append(str(llm_column).strip())

        # If any external_column is missing from DataFrame, inject lit(None).cast("string") as Config Default
        from pyspark.sql.functions import lit
        for ext_col in target_ext_cols:
            if ext_col.lower() not in seen_case_insensitive:
                df = df.withColumn(ext_col, lit(None).cast("string"))
                seen_case_insensitive.add(ext_col.lower())
                deduped_columns.append(ext_col)

        logger.info(
            f"[COLUMN RESOLUTION] Enforced column precedence & deduplicated columns "
            f"(Total unique columns: {len(deduped_columns)})."
        )
        return df.select(deduped_columns)

    @classmethod
    def _purge_s3_prefix(cls, s3_client, s3_uri: str) -> int:
        """
        Safely deletes orphaned or corrupted metadata/data files under an S3 URI.
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
    def _resolve_aws_region(cls, params: Optional[Dict[str, Any]] = None, bucket_name: Optional[str] = None) -> str:
        """Dynamically resolves the AWS region from params, environment, boto3 session, or S3 bucket location."""
        params = params or {}
        r = (
            params.get('AWS_REGION')
            or params.get('AWS_DEFAULT_REGION')
            or params.get('REGION')
            or os.environ.get('AWS_REGION')
            or os.environ.get('AWS_DEFAULT_REGION')
        )
        if r and str(r).strip():
            return str(r).strip()
        try:
            import boto3
            sess_region = boto3.Session().region_name
            if sess_region and str(sess_region).strip():
                return str(sess_region).strip()
        except Exception:
            pass

        # Try to resolve region from S3 bucket location if bucket name is available
        b = bucket_name or params.get('DATA_LAKE_BUCKET') or params.get('BUCKET_NAME')
        if b:
            clean_b = str(b).replace("s3://", "").split("/")[0].strip()
            if clean_b and not clean_b.startswith("{"):
                try:
                    import boto3
                    s3_c = params.get('_s3_client') or boto3.client('s3')
                    loc = s3_c.get_bucket_location(Bucket=clean_b).get('LocationConstraint')
                    if loc:
                        return 'eu-west-1' if loc == 'EU' else loc
                    if loc == '':
                        return 'us-east-1'
                except Exception:
                    pass

        return 'us-east-1'

    @classmethod
    def _purge_table_registration(
        cls,
        spark: SparkSession,
        glue_database: str,
        target_table_name: str,
        s3_location: str,
        glue_client=None,
        s3_client=None
    ) -> None:
        """Completely purges table from Spark SQL (PURGE), Spark catalog cache, Glue Catalog, and S3."""
        logger.info(f"[INITIAL LOAD TABLE PURGE] Purging `{glue_database}`.`{target_table_name}` at '{s3_location}'...")
        for ident in [
            f"`glue_catalog`.`{glue_database}`.`{target_table_name}`",
            f"`{glue_database}`.`{target_table_name}`",
            f"glue_catalog.{glue_database}.{target_table_name}",
            f"{glue_database}.{target_table_name}",
            target_table_name
        ]:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {ident} PURGE")
            except Exception as e:
                logger.debug(f"Spark DROP TABLE {ident} note: {e}")
        try:
            spark.catalog.clearCache()
        except Exception:
            pass
        if glue_client:
            try:
                glue_client.delete_table(DatabaseName=glue_database, Name=target_table_name)
                logger.info(f"[GLUE API] Dropped catalog table `{glue_database}`.`{target_table_name}`.")
            except Exception as e:
                logger.debug(f"[GLUE API] delete_table note for `{target_table_name}`: {e}")
        if s3_client and s3_location:
            cls._purge_s3_prefix(s3_client, s3_location)
            cls._purge_s3_prefix(s3_client, f"{s3_location.rstrip('/')}/metadata")
        try:
            spark.catalog.clearCache()
        except Exception:
            pass

    @classmethod
    def _create_iceberg_table_from_df(
        cls,
        spark: SparkSession,
        df: DataFrame,
        glue_database: str,
        target_table_name: str,
        s3_location: str,
        clean_base_name: str,
        glue_client=None,
        s3_client=None
    ) -> None:
        """Creates or recreates an Iceberg table from a DataFrame with Spark SQL DDL fallback."""
        full_table = f"`{glue_database}`.`{target_table_name}`"
        temp_view = f"init_create_{clean_base_name}"
        try:
            df.createOrReplaceTempView(temp_view)
        except Exception:
            pass
        try:
            try:
                df.write \
                    .format("iceberg") \
                    .mode("overwrite") \
                    .option("path", s3_location) \
                    .saveAsTable(f"{glue_database}.{target_table_name}")
                logger.info(f"[INITIAL LOAD WRITE] Successfully saved Iceberg table {full_table} via saveAsTable.")
                return
            except Exception as ice_err:
                err_text = str(ice_err)
                if any(x in err_text for x in (
                    "NoSuchKey", "The specified key does not exist", "NoSuchKeyException", "404",
                    "already exists", "TableAlreadyExistsException", "Cannot create table"
                )):
                    logger.warning(
                        f"[INITIAL LOAD WRITE RECOVERY] Iceberg saveAsTable hit ({ice_err}). "
                        f"Purging Glue Catalog entry, invalidating cache, and creating table fresh via Spark SQL DDL..."
                    )
                    cls._purge_table_registration(
                        spark=spark,
                        glue_database=glue_database,
                        target_table_name=target_table_name,
                        s3_location=s3_location,
                        glue_client=glue_client,
                        s3_client=s3_client
                    )
                    df.createOrReplaceTempView(temp_view)

                    create_sql_catalog = (
                        f"CREATE TABLE glue_catalog.{glue_database}.{target_table_name} "
                        f"USING iceberg "
                        f"LOCATION '{s3_location}' "
                        f"AS SELECT * FROM {temp_view}"
                    )
                    create_sql_direct = (
                        f"CREATE TABLE `{glue_database}`.`{target_table_name}` "
                        f"USING iceberg "
                        f"LOCATION '{s3_location}' "
                        f"AS SELECT * FROM {temp_view}"
                    )

                    try:
                        logger.info(f"[INITIAL LOAD WRITE DDL] Executing:\n{create_sql_catalog}")
                        spark.sql(create_sql_catalog)
                        logger.info(f"[INITIAL LOAD WRITE DDL] Successfully created Iceberg table glue_catalog.{glue_database}.{target_table_name} via Spark SQL DDL.")
                    except Exception as ddl_cat_err:
                        logger.warning(f"[INITIAL LOAD WRITE DDL NOTE] Catalog DDL note ({ddl_cat_err}). Trying database-qualified DDL...")
                        try:
                            logger.info(f"[INITIAL LOAD WRITE DDL] Executing:\n{create_sql_direct}")
                            spark.sql(create_sql_direct)
                            logger.info(f"[INITIAL LOAD WRITE DDL] Successfully created Iceberg table {full_table} via Spark SQL DDL.")
                        except Exception as ddl_dir_err:
                            logger.warning(f"[INITIAL LOAD WRITE DDL NOTE] Database DDL note ({ddl_dir_err}). Retrying saveAsTable on purged location...")
                            df.write \
                                .format("iceberg") \
                                .mode("overwrite") \
                                .option("path", s3_location) \
                                .saveAsTable(f"{glue_database}.{target_table_name}")
                            logger.info(f"[INITIAL LOAD WRITE] Successfully saved Iceberg table {full_table} via saveAsTable after purge.")
                else:
                    raise
        finally:
            try:
                spark.catalog.dropTempView(temp_view)
            except Exception:
                pass

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
                cls._purge_table_registration(
                    spark=spark,
                    glue_database=glue_database,
                    target_table_name=target_table_name,
                    s3_location=s3_location,
                    glue_client=glue_client,
                    s3_client=s3_client
                )
                return True
            return False
        except Exception as err:
            err_str = str(err)
            if any(x in err_str for x in ("NoSuchKey", "The specified key does not exist", "NoSuchKeyException", "404")):
                logger.warning(
                    f"[GHOST TABLE DETECTED IN COMPATIBILITY CHECK] Table '{full_table}' hit {err}. "
                    f"Purging ghost table registration to allow clean recreation..."
                )
                cls._purge_table_registration(
                    spark=spark,
                    glue_database=glue_database,
                    target_table_name=target_table_name,
                    s3_location=s3_location,
                    glue_client=glue_client,
                    s3_client=s3_client
                )
                return True
            logger.debug(f"[SCHEMA COMPATIBILITY CHECK] Note checking table '{full_table}': {err}")
            return False

    @classmethod
    def _get_gold_layer_manager(cls):
        """Lazily imports GoldLayerManager to prevent circular import locks."""
        global GoldLayerManager
        if 'gold_layer_manager' in sys.modules and hasattr(sys.modules['gold_layer_manager'], 'GoldLayerManager'):
            return sys.modules['gold_layer_manager'].GoldLayerManager
        if 'gold.script.gold_layer_manager' in sys.modules and hasattr(sys.modules['gold.script.gold_layer_manager'], 'GoldLayerManager'):
            return sys.modules['gold.script.gold_layer_manager'].GoldLayerManager
        if GoldLayerManager is not None:
            return GoldLayerManager
        try:
            from gold_layer_manager import GoldLayerManager as glm
            GoldLayerManager = glm
            return GoldLayerManager
        except ImportError:
            try:
                from gold.script.gold_layer_manager import GoldLayerManager as glm
                GoldLayerManager = glm
                return GoldLayerManager
            except ImportError:
                return None

    @classmethod
    def _get_protegrity_encryption_manager(cls):
        """Lazily and dynamically imports ProtegrityEncryptionManager class."""
        global ProtegrityEncryptionManager
        if ProtegrityEncryptionManager is not None:
            return ProtegrityEncryptionManager
        for p in [
            script_dir,
            os.path.abspath(os.path.join(script_dir, "..", "script")),
            os.path.abspath("gold/script"),
            os.path.abspath("bronze/script"),
            os.getcwd()
        ]:
            if os.path.isdir(p) and p not in sys.path:
                sys.path.insert(0, p)
        try:
            from protegrity_encryption import ProtegrityEncryptionManager as pem
            ProtegrityEncryptionManager = pem
            return ProtegrityEncryptionManager
        except ImportError:
            try:
                from gold.script.protegrity_encryption import ProtegrityEncryptionManager as pem
                ProtegrityEncryptionManager = pem
                return ProtegrityEncryptionManager
            except ImportError:
                return None

    @classmethod
    def run_initial_load(
        cls,
        spark: SparkSession,
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Executes initial CSV load and upserts into Gold table.
        """
        start_time = datetime.now(timezone.utc)

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

        source_system = (params.get('SOURCE_SYSTEM') or '').strip().lower()
        table_name = (params.get('TABLE_NAME') or params.get('MART_NAME') or '').strip().lower()

        if not source_system or not table_name:
            raise ValueError(
                "CRITICAL ERROR: '--SOURCE_SYSTEM' and '--TABLE_NAME' are required for Gold initial load.\n"
                "Example: --SOURCE_SYSTEM genesys --TABLE_NAME conversations"
            )

        # Clean table name
        prefixes = [f"gold_{source_system}_", f"{source_system}_", "gold_tbl_", "gold_", "v_", "tbl_", "raw_tbl_"]
        changed = True
        while changed:
            changed = False
            for prefix in prefixes:
                if table_name.startswith(prefix):
                    table_name = table_name[len(prefix):]
                    changed = True
                    break

        # Load gold_config.json with dynamic {env} interpolation
        gold_cfg = GoldConfigLoader.load_config(env=env) if GoldConfigLoader else {}
        if GoldConfigLoader:
            GoldConfigLoader.set_loaded_config(gold_cfg, env=env)
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

        # Resolve initial export/load CSV path: CLI > gold_config.json > S3 template/convention
        csv_path = params.get('CSV_PATH') or params.get('INITIAL_LOAD_PATH') or params.get('INPUT_FILE')
        if not csv_path and GoldConfigLoader:
            init_cfg = GoldConfigLoader.get_initial_load_config(source_system, table_name, gold_cfg)
            csv_path = init_cfg.get('path')
            if not params.get('DELIMITER') and init_cfg.get('delimiter'):
                params['DELIMITER'] = init_cfg.get('delimiter')
            if not params.get('HAS_HEADER') and 'has_header' in init_cfg:
                params['HAS_HEADER'] = init_cfg.get('has_header')

        # Template variable replacement (e.g. {bucket}, {env})
        if csv_path and isinstance(csv_path, str):
            csv_path = (
                csv_path
                .replace("{bucket}", bucket_name)
                .replace("{env}", env)
                .replace("{ENV}", env.upper())
                .replace("{source}", source_system)
                .replace("{table}", table_name)
            )

        # Fallback to default S3 convention if not explicitly passed
        if not csv_path:
            csv_path = f"s3://{bucket_name}/gold/initial_exports/{source_system}/{table_name}.csv"
            logger.info(f"[INITIAL LOAD CONVENTION] No explicit path passed. Using default S3 export path: '{csv_path}'")

        # Resolve Target Table Name (Default: gold_<source>_<tablename>)
        if GoldConfigLoader:
            target_table_name = GoldConfigLoader.get_target_table_name(source_system, table_name, 'athena', gold_cfg)
        else:
            clean_t = table_name.strip().lower()
            p_str = f"gold_{source_system}_"
            if clean_t.startswith(p_str):
                target_table_name = clean_t
            else:
                target_table_name = f"{p_str}{clean_t}"
        p_str = f"gold_{source_system}_"
        while target_table_name.startswith(f"{p_str}{p_str}"):
            target_table_name = target_table_name[len(p_str):]

        # Resolve Natural Keys (nkey) / Primary Keys
        pks = []
        if GoldLayerManager:
            pks = GoldLayerManager._resolve_natural_keys(source_system, table_name, gold_cfg, params)
        elif GoldConfigLoader:
            pks = GoldConfigLoader.get_nkey(source_system, table_name, gold_cfg)
        if not pks and params.get('NKEY'):
            pks = [k.strip() for k in str(params['NKEY']).split(',') if k.strip()]
        if not pks and params.get('PRIMARY_KEY'):
            pks = [k.strip() for k in str(params['PRIMARY_KEY']).split(',') if k.strip()]

        if not pks:
            raise ValueError(
                f"CRITICAL CONFIG ERROR: Missing 'nkey' in gold configuration for table '{table_name}' "
                f"under source system '{source_system}'. A natural key is mandatory for initial load."
            )

        full_table = f"`{glue_database}`.`{target_table_name}`"

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
            f"|  * Execution Time    : {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"+================================================================================+"
        )

        # 1. Read Input File (CSV or Parquet)
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
                .option("inferSchema", "false") \
                .csv(csv_path, quote='"', escape='"', multiLine=True)

        raw_count = df_raw.count()
        try:
            logger.info(f"Loaded {int(raw_count):,} records from input file.")
        except Exception:
            logger.info(f"Loaded {raw_count} records from input file.")

        # Cleanse column names in df_raw immediately (ensures column names align with encryption_columns)
        for orig_col in list(df_raw.columns):
            clean_col = cls.sanitize_column_name(orig_col)
            if clean_col != orig_col:
                logger.info(f"[COLUMN CLEANSING] Renaming '{orig_col}' -> '{clean_col}'")
                df_raw = df_raw.withColumnRenamed(orig_col, clean_col)

        # Protegrity Encryption for Historical Initial Load Data
        table_cfg = GoldConfigLoader.get_table_config(source_system, table_name, gold_cfg) if GoldConfigLoader else {}
        init_cfg = GoldConfigLoader.get_initial_load_config(source_system, table_name, gold_cfg) if GoldConfigLoader else {}
        defaults_cfg = GoldConfigLoader.get_defaults(gold_cfg, env=env) if GoldConfigLoader else {}

        # 1. Resolve encryption columns
        mart_enc_cols = (
            params.get("ENCRYPTION_COLUMNS")
            or params.get("ENCRYPT_COLUMNS")
            or table_cfg.get("encryption_columns")
            or table_cfg.get("encryption", {}).get("columns")
            or init_cfg.get("encryption_columns")
            or init_cfg.get("encryption", {}).get("columns")
        )
        if isinstance(mart_enc_cols, str) and (mart_enc_cols.strip().startswith("{") or mart_enc_cols.strip().startswith("[")):
            try:
                import json
                mart_enc_cols = json.loads(mart_enc_cols.strip())
            except Exception:
                pass

        # 2. Merge encryption settings (defaults < table_cfg < params)
        enc_defaults = dict(defaults_cfg.get("encryption", {}))
        if "encryption" in table_cfg and isinstance(table_cfg["encryption"], dict):
            enc_defaults.update({k: v for k, v in table_cfg["encryption"].items() if k != "columns"})
        if "encryption" in init_cfg and isinstance(init_cfg["encryption"], dict):
            enc_defaults.update({k: v for k, v in init_cfg["encryption"].items() if k != "columns"})

        cli_enabled = (
            str(params.get('ENABLE_ENCRYPTION', '')).strip().lower() in ('true', '1', 'yes')
            or str(params.get('ENCRYPTION_ENABLED', '')).strip().lower() in ('true', '1', 'yes')
        )
        cli_disabled = (
            str(params.get('ENABLE_ENCRYPTION', '')).strip().lower() in ('false', '0', 'no')
            or str(params.get('ENCRYPTION_ENABLED', '')).strip().lower() in ('false', '0', 'no')
        )
        cli_lambda_arn = params.get('PROTEGRITY_LAMBDA_ARN') or params.get('LAMBDA_ARN')
        cli_secret_name = params.get('SECRET_NAME') or params.get('PROTEGRITY_SECRET_NAME')
        cli_enc_key = params.get('ENCRYPTION_KEY') or params.get('USER_SECRET')

        if cli_lambda_arn:
            enc_defaults["lambda_arn"] = cli_lambda_arn
        if cli_secret_name:
            enc_defaults["secret_name"] = cli_secret_name
        if cli_enc_key:
            enc_defaults["encryption_key"] = cli_enc_key

        has_lambda_arn = bool(enc_defaults.get("lambda_arn") or enc_defaults.get("protegrity_lambda_arn"))
        config_enabled = bool(enc_defaults.get("enabled", False))

        should_encrypt = (
            not cli_disabled
            and bool(mart_enc_cols)
            and (cli_enabled or has_lambda_arn or config_enabled)
        )
        if should_encrypt:
            enc_defaults["enabled"] = True

        enc_cls = cls._get_protegrity_encryption_manager()
        if should_encrypt and mart_enc_cols and enc_cls:
            try:
                enc_mgr = enc_cls(global_config={"encryption": enc_defaults}, env=env)
                logger.info(
                    f"[GOLD ENCRYPTION] Encrypting PII columns "
                    f"{list(mart_enc_cols.keys()) if isinstance(mart_enc_cols, dict) else mart_enc_cols} "
                    f"using Protegrity Lambda for initial load table '{source_system}.{table_name}'..."
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

        s3_location = f"s3://{bucket_name}/gold/data/{source_system}/{table_name}"
        glue_client = params.get('_glue_client')
        s3_client = params.get('_s3_client')
        if glue_client is None or s3_client is None:
            try:
                import boto3
                region = cls._resolve_aws_region(params, bucket_name=bucket_name)
                if glue_client is None:
                    glue_client = boto3.client('glue', region_name=region)
                if s3_client is None:
                    s3_client = boto3.client('s3', region_name=region)
            except Exception:
                pass

        # 2. Check Target Table Schema for Reconciliation & Detect Ghost Tables
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
                import urllib.parse
                parsed = urllib.parse.urlparse(glue_metadata_location)
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
                    cls._purge_table_registration(
                        spark=spark,
                        glue_database=glue_database,
                        target_table_name=target_table_name,
                        s3_location=s3_location,
                        glue_client=glue_client,
                        s3_client=s3_client
                    )
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
                cls._purge_table_registration(
                    spark=spark,
                    glue_database=glue_database,
                    target_table_name=target_table_name,
                    s3_location=s3_location,
                    glue_client=glue_client,
                    s3_client=s3_client
                )
                table_exists = False
        else:
            table_exists = False
            logger.info(f"Gold table '{full_table}' not found in catalog. Attempting automatic Gold query extraction...")

        # If table not yet in catalog, probe target schema from Gold SQL query
        if not target_fields:
            target_fields = cls._probe_target_schema_from_query(
                spark=spark,
                source_system=source_system,
                table_name=table_name,
                params=params
            )

        # 3. Reconcile Schemas
        df_consolidated = cls.reconcile_schema(df_raw, target_fields)

        # Custom Transform Invocation for Initial Load (Historical pre-enriched data)
        external_cols = GoldConfigLoader.get_external_columns(source_system, table_name, gold_cfg) if GoldConfigLoader else []
        primary_ext_col = external_cols[0] if external_cols else None
        custom_script_path = GoldConfigLoader.get_custom_transform_path(source_system, table_name, gold_cfg) if GoldConfigLoader else None

        if custom_script_path:
            api_secret_name = (
                params.get('API_SECRET_NAME')
                or params.get('api_secret_name')
                or params.get('LLM_SECRET_NAME')
                or params.get('llm_secret_name')
            )
            if not api_secret_name and GoldConfigLoader:
                api_secret_name = GoldConfigLoader.get_api_secret_name(source_system, table_name, gold_cfg)

            context = {
                "source_system": source_system,
                "table_name": table_name,
                "target_table": target_table_name,
                "glue_database": glue_database,
                "primary_keys": pks,
                "is_initial_load": True,
                "is_incremental": False,
                "external_columns": external_cols,
                "llm_column": primary_ext_col,
                "api_secret_name": api_secret_name,
                "params": params,
                "bucket_name": bucket_name,
                "_s3_client": s3_client
            }
            df_consolidated = cls._apply_custom_transform(
                df_consolidated, custom_script_path, spark=spark, context=context
            )

        # Enforce Column Conflict Resolution & Deduplication (Transform > SQL > Config)
        df_consolidated = cls._resolve_and_deduplicate_columns(
            df_consolidated, primary_keys=pks, external_columns=external_cols
        )

        # 4. Attach Technical Audit Columns
        if "_updated_at" not in df_consolidated.columns:
            df_consolidated = df_consolidated.withColumn("_updated_at", to_timestamp(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")))
        if "_inserted_at" not in df_consolidated.columns:
            df_consolidated = df_consolidated.withColumn("_inserted_at", to_timestamp(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")))

        # Ensure row uniqueness by natural keys (nkey) from gold config
        df_consolidated = cls._deduplicate_by_nkey(df_consolidated, pks)
        logger.info(f"[PRIMARY KEY] Natural keys resolved for '{table_name}': {pks}")

        # 5. Perform Idempotent UPSERT into Athena / Iceberg Table
        # 5. Perform Idempotent UPSERT into Athena / Iceberg Table
        temp_view = f"incoming_initial_{table_name}"
        df_consolidated.createOrReplaceTempView(temp_view)

        glm = cls._get_gold_layer_manager()
        if table_exists:
            # Check for column type incompatibility (e.g. existing table has bigint while incoming has string)
            was_purged = cls._check_and_resolve_iceberg_incompatibilities(
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
                if glm and hasattr(glm, "_sync_iceberg_schema"):
                    glm._sync_iceberg_schema(spark, full_table, df_consolidated)
                elif hasattr(cls, "_sync_iceberg_schema"):
                    cls._sync_iceberg_schema(spark, full_table, df_consolidated)
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
                    cls._create_iceberg_table_from_df(
                        spark=spark,
                        df=df_consolidated,
                        glue_database=glue_database,
                        target_table_name=target_table_name,
                        s3_location=s3_location,
                        clean_base_name=table_name,
                        glue_client=glue_client,
                        s3_client=s3_client
                    )
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
                            cls._create_iceberg_table_from_df(
                                spark=spark,
                                df=df_consolidated,
                                glue_database=glue_database,
                                target_table_name=target_table_name,
                                s3_location=s3_location,
                                clean_base_name=table_name,
                                glue_client=glue_client,
                                s3_client=s3_client
                            )
                        else:
                            logger.error(f"[INITIAL LOAD UPSERT ERROR] Iceberg append fallback also failed: {ice_fallback_err}")
                            raise
        else:
            logger.info(f"[INITIAL LOAD WRITE] Initializing clean Gold Iceberg table {full_table} at '{s3_location}'...")
            cls._create_iceberg_table_from_df(
                spark=spark,
                df=df_consolidated,
                glue_database=glue_database,
                target_table_name=target_table_name,
                s3_location=s3_location,
                clean_base_name=table_name,
                glue_client=glue_client,
                s3_client=s3_client
            )

        # 6. Optional Downstream Extension: Aurora MySQL Serving
        # Only created/served if requested by user/config (target_engines contains 'aurora' or --GOLD_TARGETS contains 'aurora')
        target_engines = []
        if params.get('GOLD_TARGETS') or params.get('TARGET_ENGINES'):
            target_engines = [t.strip().lower() for t in str(params.get('GOLD_TARGETS') or params.get('TARGET_ENGINES')).split(',') if t.strip()]
        elif GoldConfigLoader:
            target_engines = GoldConfigLoader.get_target_engines(source_system, table_name, gold_cfg)

        skip_aurora = str(params.get('SKIP_AURORA_SERVE', 'false')).strip().lower() in ('true', '1', 'yes')
        needs_aurora = not skip_aurora and any(t in target_engines for t in ('aurora', 'rds', 'mysql'))
        if needs_aurora and glm:
            logger.info(f"[AURORA EXTENSION] User requested Aurora serving for '{target_table_name}'. Reading from Athena table and serving to Aurora...")
            try:
                try:
                    df_athena = spark.table(full_table)
                except Exception as read_tbl_err:
                    logger.warning(f"Could not read from spark.table({full_table}): {read_tbl_err}. Using consolidated DataFrame directly.")
                    df_athena = df_consolidated
                gold_schema = params.get('RDS_GOLD_SCHEMA')
                if not gold_schema:
                    raise ValueError(
                        "CRITICAL CONFIG ERROR: Missing required parameter '--RDS_GOLD_SCHEMA'.\n"
                        "Aurora serving requires an explicit target schema. Please specify: --RDS_GOLD_SCHEMA <schema_name>"
                    )
                glm._serve_to_mysql(
                    spark=spark,
                    queries={table_name: ""},
                    gold_schema=gold_schema,
                    data_s3_path=s3_location,
                    params=params,
                    glue_client=None,
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
            f"|  * Records Loaded  : {f'{int(raw_count):,}' if isinstance(raw_count, (int, float)) else raw_count}\n"
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


def main():
    """Glue entrypoint when executed as a Glue Job."""
    from pyspark import SparkConf
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    expected_args = [
        'JOB_NAME', 'SOURCE_SYSTEM', 'TABLE_NAME'
    ]
    optional_args = [
        'CSV_PATH', 'NKEY', 'PRIMARY_KEY', 'SECRET_NAME', 'RDS_SECRET_NAME',
        'API_SECRET_NAME', 'LLM_SECRET_NAME',
        'GLUE_DATABASE', 'DATA_LAKE_BUCKET', 'GOLD_BUCKET', 'DELIMITER', 'HAS_HEADER',
        'ENV', 'ENVIRONMENT', 'INITIAL_LOAD_PATH', 'INPUT_FILE', 'MART_NAME',
        'ENABLE_ENCRYPTION', 'ENCRYPTION_ENABLED', 'PROTEGRITY_LAMBDA_ARN', 'LAMBDA_ARN',
        'ENCRYPTION_KEY', 'USER_SECRET_KEY', 'USER_SECRET', 'PROTEGRITY_SECRET_NAME',
        'ENCRYPTION_COLUMNS', 'ENCRYPT_COLUMNS', 'GOLD_TARGETS', 'TARGET_ENGINES',
        'SKIP_AURORA_SERVE', 'RDS_GOLD_SCHEMA', 'AWS_REGION'
    ]

    args_to_check = expected_args + [a for a in optional_args if f"--{a}" in sys.argv or f"--{a.lower()}" in sys.argv]
    resolved = getResolvedOptions(sys.argv, args_to_check)

    bucket_name = resolved.get('DATA_LAKE_BUCKET') or resolved.get('GOLD_BUCKET') or 'uax-datalake-dev-bucket'
    env_val = resolved.get('ENV') or 'dev'
    clean_bucket = str(bucket_name).replace('{env}', env_val).strip()

    region_val = GoldInitialLoader._resolve_aws_region(resolved, bucket_name=clean_bucket)

    conf = SparkConf()
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{clean_bucket}/gold/data/")
    conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set("spark.sql.catalog.glue_catalog.client.region", region_val)
    conf.set("spark.sql.catalog.glue_catalog.glue.region", region_val)
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
        ("spark.sql.catalog.glue_catalog.client.region", region_val),
        ("spark.sql.catalog.glue_catalog.glue.region", region_val),
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

    GoldInitialLoader.run_initial_load(spark, resolved)


if __name__ == "__main__":
    main()
