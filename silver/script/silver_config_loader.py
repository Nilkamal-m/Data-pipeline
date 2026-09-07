"""
Silver Layer Configuration Loader for PySpark Apache Iceberg ETL.

Loads configuration from local file or S3 path (s3://<bucket>/scripts/silver/config/silver_config.json).
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
        """
        source_cfg = cls.get_source_config(source_system, config_dict)
        return source_cfg.get("default_tables", [])

    @classmethod
    def get_table_config(cls, source_system: str, table_name: str, config_dict: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Retrieves table-specific configuration (nkey, order_by, scd_type, etc.).
        Supports lookup by raw_tbl_<table_name> or base <table_name>.
        """
        source_cfg = cls.get_source_config(source_system, config_dict)
        table_configs = source_cfg.get("table_configs", {})
        clean = table_name.strip().lower()
        if clean in table_configs:
            return table_configs[clean]
        if clean.startswith("raw_tbl_"):
            base = clean[len("raw_tbl_"):]
            if base in table_configs:
                return table_configs[base]
        else:
            prefixed = f"raw_tbl_{clean}"
            if prefixed in table_configs:
                return table_configs[prefixed]
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
    def get_technical_columns(cls, config_dict: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Retrieves technical columns configuration from silver_defaults.
        """
        config = config_dict or cls._config_cache or cls.load_config()
        defaults = config.get("silver_defaults", {})
        return defaults.get("technical_columns", {})

    @classmethod
    def get_table_prefix(cls, config_dict: Optional[Dict[str, Any]] = None) -> str:
        """
        Retrieves table prefix for Silver layer tables from centralized silver_defaults.
        Raises ValueError if table_prefix is missing or empty.
        """
        config = config_dict or cls._config_cache or cls.load_config()
        defaults = config.get("silver_defaults", {})
        prefix = defaults.get("table_prefix")
        if not prefix or not str(prefix).strip():
            raise ValueError(
                "CRITICAL CONFIG ERROR: 'table_prefix' is missing or empty in silver_config.json "
                "(silver_defaults.table_prefix). Please configure it (e.g. 'tbl_')."
            )
        return str(prefix).strip()

    @classmethod
    def get_glue_database(cls, config_dict: Optional[Dict[str, Any]] = None) -> str:
        """
        Retrieves glue_database for Silver layer tables from centralized silver_defaults.
        Raises ValueError if glue_database is missing or empty.
        """
        config = config_dict or cls._config_cache or cls.load_config()
        defaults = config.get("silver_defaults", {})
        db = defaults.get("glue_database")
        if not db or not str(db).strip():
            raise ValueError(
                "CRITICAL CONFIG ERROR: 'glue_database' is missing or empty in silver_config.json "
                "(silver_defaults.glue_database). Please configure it (e.g. 'uax-datalake-db-dev')."
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
        Resolves the full Silver Iceberg table identifier (e.g. uax-datalake-db-dev.tbl_incident).
        Allows table-specific override via 'target_table_name' in table_configs.
        Pulls table_prefix and glue_database from centralized config if not provided.
        Strips 'raw_tbl_' prefix from input table_name so output is always tbl_<base_name>.
        """
        prefix = table_prefix or cls.get_table_prefix(config_dict)
        db = glue_database or cls.get_glue_database(config_dict)
        table_cfg = cls.get_table_config(source_system, table_name, config_dict)
        table_clean = table_name.strip().lower()
        base_name = table_clean[len("raw_tbl_"):] if table_clean.startswith("raw_tbl_") else table_clean
        target_name = table_cfg.get("target_table_name") or f"{prefix}{base_name}"
        return f"{db}.{target_name}"

    @classmethod
    def get_watermark_config(cls, config_dict: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Retrieves watermark configuration from silver_defaults.
        """
        config = config_dict or cls._config_cache or cls.load_config()
        defaults = config.get("silver_defaults", {})
        return defaults.get("watermark", {
            "enabled": True,
            "metadata_prefix": "metadata/silver",
            "watermark_column": "_ingested_at",
            "sync_watermark_table": True,
            "watermark_table_name": "tbl_watermarks",
            "full_refresh": False
        })

    @classmethod
    def is_watermark_enabled(cls, config_dict: Optional[Dict[str, Any]] = None) -> bool:
        """
        Returns whether watermark tracking is enabled.
        """
        return cls.get_watermark_config(config_dict).get("enabled", True)

    @classmethod
    def get_watermark_table_name(cls, config_dict: Optional[Dict[str, Any]] = None) -> str:
        """
        Retrieves watermark_table_name from silver_defaults.watermark.
        Raises ValueError if watermark_table_name is missing or empty.
        """
        wm_cfg = cls.get_watermark_config(config_dict)
        tbl_name = wm_cfg.get("watermark_table_name")
        if not tbl_name or not str(tbl_name).strip():
            raise ValueError(
                "CRITICAL CONFIG ERROR: 'watermark_table_name' is missing or empty in silver_config.json "
                "(silver_defaults.watermark.watermark_table_name). Please configure it (e.g. 'tbl_watermarks')."
            )
        return str(tbl_name).strip()

