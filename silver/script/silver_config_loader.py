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
        Retrieves table-specific configuration (primary_key, order_by).
        """
        source_cfg = cls.get_source_config(source_system, config_dict)
        table_configs = source_cfg.get("table_configs", {})
        return table_configs.get(table_name.strip().lower(), {})
