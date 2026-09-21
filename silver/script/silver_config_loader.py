"""
Silver Layer Configuration Loader for PySpark Apache Iceberg ETL.

Loads configuration from local file or S3 path (s3://<bucket>/scripts/silver/config/silver_config.json).
Supports uniform config structure with pipeline_defaults, glue_catalog, and backward-compatibility with silver_defaults.
Tables are keyed by Silver name tbl_<tablename> with source_table_name raw_tbl_<tablename>.
"""

import os
import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class SilverConfigLoader:
    """
    Centralized Configuration Loader for Silver PySpark Apache Iceberg Transformation Jobs.
    """
    _config_cache: Optional[Dict[str, Any]] = None

    @classmethod
    def load_config(cls, config_s3_path: Optional[str] = None, s3_client: Optional[Any] = None) -> Dict[str, Any]:
        """
        Loads the Silver configuration JSON file from S3 or local disk.
        """
        if cls._config_cache is not None:
            return cls._config_cache

        if config_s3_path and config_s3_path.startswith("s3://") and s3_client:
            try:
                path_parts = config_s3_path.replace("s3://", "").split("/", 1)
                bucket_name, object_key = path_parts[0], path_parts[1]
                logger.info(f"Loading Silver configuration from S3: '{config_s3_path}'")
                response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
                content = response['Body'].read().decode('utf-8')
                cls._config_cache = json.loads(content)
                return cls._config_cache
            except Exception as err:
                logger.warning(f"Failed to load Silver config from S3 path '{config_s3_path}': {err}. Falling back to local config.")

        current_dir = os.path.dirname(os.path.abspath(__file__))
        possible_paths = [
            os.path.join(current_dir, "config", "silver_config.json"),
            "silver/script/config/silver_config.json",
            "glue_jobs/silver/config/silver_config.json"
        ]

        for path in possible_paths:
            if os.path.exists(path):
                logger.info(f"Loading Silver configuration from local file: '{path}'")
                with open(path, "r", encoding="utf-8") as f:
                    cls._config_cache = json.load(f)
                    return cls._config_cache

        logger.error("Silver configuration file 'silver_config.json' not found.")
        return {}

    @classmethod
    def get_defaults(cls, config_dict: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Retrieves defaults block, supporting both pipeline_defaults and silver_defaults.
        """
        config = config_dict or cls._config_cache or cls.load_config()
        return config.get("pipeline_defaults") or config.get("silver_defaults", {})

    @classmethod
    def get_source_config(cls, source_system: str, config_dict: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Retrieves configuration for a specific source system in Silver layer.
        """
        config = config_dict or cls._config_cache or cls.load_config()
        sources = config.get("source_systems", {})
        return sources.get(source_system.strip().lower(), {})

    @classmethod
    def get_default_tables(cls, source_system: str, config_dict: Optional[Dict[str, Any]] = None) -> list:
        """
        Retrieves default tables for a source system.
        If default_tables is not explicitly present, discovers all tables under 'tables' (or 'table_configs').
        """
        source_cfg = cls.get_source_config(source_system, config_dict)
        default_tables = source_cfg.get("default_tables")
        if default_tables is not None:
            return default_tables
        tables = source_cfg.get("tables") or source_cfg.get("table_configs", {})
        return list(tables.keys())

    @classmethod
    def get_table_config(cls, source_system: str, table_name: str, config_dict: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Retrieves table-specific configuration (nkey, order_by, scd_type, etc.).
        Supports lookup in both 'tables' and 'table_configs'.
        Supports lookup by tbl_<name>, raw_tbl_<name>, or base <name> with hyphens or underscores.
        """
        source_cfg = cls.get_source_config(source_system, config_dict)
        table_configs = source_cfg.get("tables") or source_cfg.get("table_configs", {})
        clean = table_name.strip().lower()

        base = clean
        if base.startswith("raw_tbl_"):
            base = base[len("raw_tbl_"):]
        elif base.startswith("tbl_"):
            base = base[len("tbl_"):]

        candidates = [
            clean,
            f"tbl_{base}",
            f"raw_tbl_{base}",
            base
        ]

        expanded_candidates = []
        for cand in candidates:
            if cand not in expanded_candidates:
                expanded_candidates.append(cand)
            if '-' in cand:
                alt = cand.replace('-', '_')
                if alt not in expanded_candidates:
                    expanded_candidates.append(alt)
            if '_' in cand:
                alt = cand.replace('_', '-')
                if alt not in expanded_candidates:
                    expanded_candidates.append(alt)

        for cand in expanded_candidates:
            if cand in table_configs:
                cfg = dict(table_configs[cand])
                if "target_table_name" not in cfg:
                    cfg["target_table_name"] = cand if cand.startswith("tbl_") else f"tbl_{base}"
                return cfg
        return {}

    @classmethod
    def get_nkey(cls, source_system: str, table_name: str, config_dict: Optional[Dict[str, Any]] = None):
        """
        Retrieves natural key (nkey) for a table with fallback to deduplication_keys or primary_key.
        Returns a string or list of strings.
        """
        table_cfg = cls.get_table_config(source_system, table_name, config_dict)
        return table_cfg.get('nkey') or table_cfg.get('deduplication_keys') or table_cfg.get('primary_key')

    @classmethod
    def get_nkey_list(cls, source_system: str, table_name: str, config_dict: Optional[Dict[str, Any]] = None) -> list:
        """
        Retrieves natural key(s) as a normalized list of strings.
        Supports single string, comma-separated string, or list of strings.
        """
        nkey = cls.get_nkey(source_system, table_name, config_dict)
        if not nkey:
            return []
        if isinstance(nkey, list):
            return [str(k).strip() for k in nkey if str(k).strip()]
        if isinstance(nkey, str) and ',' in nkey:
            return [k.strip() for k in nkey.split(',') if k.strip()]
        return [str(nkey).strip()]

    @classmethod
    def get_bronze_technical_columns(cls, config_dict: Optional[Dict[str, Any]] = None) -> list:
        """
        Retrieves list of Bronze technical/system audit columns to exclude when reading Bronze payloads into Silver.
        Bronze only adds: _ingested_at, _source_system, _table_name, _execution_id.
        """
        defaults = cls.get_defaults(config_dict)
        configured = defaults.get("bronze_technical_columns")
        if configured and isinstance(configured, list):
            return [c.strip() for c in configured if str(c).strip()]
        return [
            "_ingested_at", "_source_system", "_table_name", "_execution_id"
        ]

    @classmethod
    def get_exclude_columns(cls, source_system: str, table_name: str, config_dict: Optional[Dict[str, Any]] = None) -> list:
        """
        Retrieves list of columns to exclude from Silver table.
        Combines defaults.exclude_columns with table.exclude_columns (or drop_columns).
        """
        config = config_dict or cls._config_cache or cls.load_config()
        defaults = cls.get_defaults(config)
        default_excludes = defaults.get("exclude_columns") or defaults.get("drop_columns") or []
        if isinstance(default_excludes, str):
            default_excludes = [c.strip() for c in default_excludes.split(',') if c.strip()]

        table_cfg = cls.get_table_config(source_system, table_name, config)
        table_excludes = table_cfg.get("exclude_columns") or table_cfg.get("drop_columns") or []
        if isinstance(table_excludes, str):
            table_excludes = [c.strip() for c in table_excludes.split(',') if c.strip()]

        combined = list(dict.fromkeys(default_excludes + table_excludes))
        return combined

    @classmethod
    def get_external_columns(cls, source_system: str, table_name: str, config_dict: Optional[Dict[str, Any]] = None) -> list:
        """
        Retrieves list of external columns configured for Silver table.
        Combines defaults.external_columns with table.external_columns.
        """
        config = config_dict or cls._config_cache or cls.load_config()
        defaults = cls.get_defaults(config)
        default_ext = defaults.get("external_columns") or defaults.get("api_enrichment_columns") or defaults.get("api_columns") or []
        if isinstance(default_ext, str):
            default_ext = [c.strip() for c in default_ext.split(',') if c.strip()]

        table_cfg = cls.get_table_config(source_system, table_name, config)
        table_ext = table_cfg.get("external_columns") or table_cfg.get("api_enrichment_columns") or table_cfg.get("api_columns") or []
        if isinstance(table_ext, str):
            table_ext = [c.strip() for c in table_ext.split(',') if c.strip()]

        combined = list(dict.fromkeys(default_ext + table_ext))
        return combined

    @classmethod
    def get_technical_columns(cls, config_dict: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Retrieves technical columns configuration from defaults.
        """
        defaults = cls.get_defaults(config_dict)
        return defaults.get("technical_columns", {})

    @classmethod
    def get_table_prefix(cls, config_dict: Optional[Dict[str, Any]] = None) -> str:
        """
        Retrieves table prefix for Silver layer tables from centralized defaults.
        Raises ValueError if table_prefix is missing or empty.
        """
        defaults = cls.get_defaults(config_dict)
        prefix = defaults.get("table_prefix") or defaults.get("glue_catalog", {}).get("table_prefix")
        if not prefix or not str(prefix).strip():
            raise ValueError(
                "CRITICAL CONFIG ERROR: 'table_prefix' is missing or empty in silver_config.json "
                "(pipeline_defaults.table_prefix or glue_catalog.table_prefix). Please configure it (e.g. 'tbl_')."
            )
        return str(prefix).strip()

    @classmethod
    def get_glue_database(cls, config_dict: Optional[Dict[str, Any]] = None) -> str:
        """
        Retrieves glue_database for Silver layer tables from centralized defaults.
        Raises ValueError if glue_database is missing or empty.
        """
        defaults = cls.get_defaults(config_dict)
        db = defaults.get("glue_database") or defaults.get("glue_catalog", {}).get("database_name")
        if not db or not str(db).strip():
            raise ValueError(
                "CRITICAL CONFIG ERROR: 'glue_database' is missing or empty in silver_config.json "
                "(pipeline_defaults.glue_database or glue_catalog.database_name). Please configure it (e.g. 'uax_datalake_db_dev')."
            )
        return str(db).strip()

    @classmethod
    def get_silver_table_name(
        cls,
        source_system: str,
        table_name: str,
        glue_database: Optional[str] = None,
        table_prefix: Optional[str] = None,
        config_dict: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Resolves the full Silver Iceberg table identifier (e.g. uax_datalake_db_dev.tbl_incident).
        Allows table-specific override via 'target_table_name' in table_configs or tables.
        Pulls table_prefix and glue_database from centralized config if not provided.
        Strips 'raw_tbl_' or 'tbl_' prefix from input table_name so output is always tbl_<base_name>.
        """
        prefix = table_prefix or cls.get_table_prefix(config_dict)
        db = glue_database or cls.get_glue_database(config_dict)
        table_cfg = cls.get_table_config(source_system, table_name, config_dict)
        table_clean = table_name.strip().lower()

        base_name = table_clean
        if base_name.startswith("raw_tbl_"):
            base_name = base_name[len("raw_tbl_"):]
        elif base_name.startswith("tbl_"):
            base_name = base_name[len("tbl_"):]
        base_name = base_name.replace("-", "_")

        target_name = (table_cfg.get("target_table_name") or f"{prefix}{base_name}").replace("-", "_")
        return f"{db}.{target_name}"

    @classmethod
    def get_watermark_config(cls, config_dict: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Retrieves watermark configuration from defaults.
        """
        defaults = cls.get_defaults(config_dict)
        wm = defaults.get("watermark", {
            "enabled": True,
            "metadata_prefix": "metadata/silver",
            "watermark_column": "_ingested_at",
            "sync_watermark_table": True,
            "watermark_table_name": "tbl_watermarks",
            "full_refresh": False
        })
        if not wm.get("watermark_table_name"):
            wm["watermark_table_name"] = defaults.get("glue_catalog", {}).get("watermark_table_name", "tbl_watermarks")
        return wm

    @classmethod
    def is_watermark_enabled(cls, config_dict: Optional[Dict[str, Any]] = None) -> bool:
        """
        Returns whether watermark tracking is enabled.
        """
        return cls.get_watermark_config(config_dict).get("enabled", True)

    @classmethod
    def get_watermark_table_name(cls, config_dict: Optional[Dict[str, Any]] = None) -> str:
        """
        Retrieves watermark_table_name from defaults.watermark or glue_catalog.
        Raises ValueError if watermark_table_name is missing or empty.
        """
        wm_cfg = cls.get_watermark_config(config_dict)
        tbl_name = wm_cfg.get("watermark_table_name")
        if not tbl_name or not str(tbl_name).strip():
            raise ValueError(
                "CRITICAL CONFIG ERROR: 'watermark_table_name' is missing or empty in silver_config.json "
                "(pipeline_defaults.watermark.watermark_table_name or glue_catalog.watermark_table_name). Please configure it (e.g. 'tbl_watermarks')."
            )
        return str(tbl_name).strip()
