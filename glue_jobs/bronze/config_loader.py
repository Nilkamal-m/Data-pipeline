"""
Centralized Configuration Loader for AWS Glue Data Pipeline (Bronze Layer).

Supports:
- Table-specific initial_load_dates per table (Strictly enforces non-null initial load date).
- Dynamic Custom Table overrides and custom API endpoints.
- Table-specific Custom Query overrides merged with High-Water Mark timestamps.
- Loading local bronze_config.json or fetching dynamic S3 configuration overrides.
"""

import os
import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class ConfigLoader:
    """
    Centralized Configuration Loader supporting custom tables, table-specific load dates, and query overrides.
    """
    _config_cache: Optional[Dict[str, Any]] = None

    @classmethod
    def load_config(cls, config_s3_path: Optional[str] = None, s3_client: Optional[Any] = None) -> Dict[str, Any]:
        """
        Loads the centralized Bronze configuration JSON file.
        """
        if cls._config_cache is not None:
            return cls._config_cache

        if config_s3_path and config_s3_path.startswith("s3://") and s3_client:
            try:
                path_parts = config_s3_path.replace("s3://", "").split("/", 1)
                bucket_name, object_key = path_parts[0], path_parts[1]
                logger.info(f"Loading Bronze configuration from S3: '{config_s3_path}'")
                response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
                content = response['Body'].read().decode('utf-8')
                cls._config_cache = json.loads(content)
                return cls._config_cache
            except Exception as err:
                logger.warning(f"Failed to load config from S3 path '{config_s3_path}': {err}. Falling back to local config.")

        current_dir = os.path.dirname(os.path.abspath(__file__))
        possible_paths = [
            os.path.join(current_dir, "config", "bronze_config.json"),
            "glue_jobs/bronze/config/bronze_config.json"
        ]

        for path in possible_paths:
            if os.path.exists(path):
                logger.info(f"Loading Bronze configuration from local file: '{path}'")
                with open(path, "r", encoding="utf-8") as f:
                    cls._config_cache = json.load(f)
                    return cls._config_cache

        logger.error("Bronze configuration file 'bronze_config.json' not found.")
        raise FileNotFoundError("Bronze configuration file 'bronze_config.json' not found.")

    @classmethod
    def get_source_config(cls, source_system: str, config_dict: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Retrieves configuration for a specific source system.
        """
        config = config_dict or cls._config_cache or cls.load_config()
        sources = config.get("source_systems", {})
        source_key = source_system.strip().lower()

        if source_key not in sources:
            logger.warning(f"Source system '{source_system}' not found in configuration. Returning empty defaults.")
            return {}

        return sources[source_key]

    @classmethod
    def get_table_initial_load_date(
        cls,
        source_system: str,
        table_name: str,
        cli_initial_date: Optional[str] = None,
        source_config: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Retrieves the table-specific initial_load_date configured in bronze_config.json or CLI.
        Raises ValueError if no initial load date is defined (Fallback load dates strictly disabled).
        """
        table_clean = table_name.strip()

        # 1. Check table_initial_load_dates in bronze_config.json
        config = source_config or cls.get_source_config(source_system)
        table_dates = config.get("table_initial_load_dates", {})
        if table_clean in table_dates and table_dates[table_clean] and str(table_dates[table_clean]).strip():
            date_val = str(table_dates[table_clean]).strip()
            logger.info(f"Using configured initial_load_date for table '{table_clean}': {date_val}")
            return date_val

        # 2. Check CLI passed initial date
        if cli_initial_date and cli_initial_date.strip():
            logger.info(f"Using CLI passed initial_load_date for table '{table_clean}': {cli_initial_date.strip()}")
            return cli_initial_date.strip()

        # 3. Check global default_initial_load_date in bronze_config.json defaults
        global_default = cls.get_default_setting("default_initial_load_date", None)
        if global_default and str(global_default).strip():
            logger.info(f"Using global initial_load_date for table '{table_clean}': {global_default.strip()}")
            return global_default.strip()

        # 4. Strict Enforcement: Throw error if load date is NULL/missing
        err_msg = (
            f"CRITICAL ERROR: No initial load date specified for table '{table_clean}' in source '{source_system}'. "
            f"Fallback load dates are disabled to prevent loading unwanted past records. "
            f"Please configure 'table_initial_load_dates' for '{table_clean}' in bronze_config.json or pass '--INITIAL_LOAD_DATE'."
        )
        logger.error(err_msg)
        raise ValueError(err_msg)

    @classmethod
    def get_table_query_filter(
        cls,
        source_system: str,
        table_name: str,
        last_load_date: str,
        custom_query_cli: Optional[str] = None,
        source_config: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Resolves the final query filter for a table by combining CLI query overrides, table query overrides, and default delta filters.
        """
        config = source_config or cls.get_source_config(source_system)
        table_clean = table_name.strip()

        if custom_query_cli and custom_query_cli.strip():
            cli_query = custom_query_cli.strip()
            if "sys_updated_on" not in cli_query and "updated_at" not in cli_query:
                delta_part = f"sys_updated_on>={last_load_date}" if source_system == 'servicenow' else f"updated_at gt '{last_load_date}'"
                final_filter = f"{cli_query}^{delta_part}" if source_system == 'servicenow' else f"{cli_query} and {delta_part}"
            else:
                final_filter = cli_query.replace("{last_load_date}", last_load_date)
            logger.info(f"Using CLI Custom Query override for table '{table_clean}': {final_filter}")
            return final_filter

        table_overrides = config.get("table_query_overrides", {})
        if table_clean in table_overrides and table_overrides[table_clean]:
            configured_query = table_overrides[table_clean].replace("{last_load_date}", last_load_date)
            logger.info(f"Using Configured Table Query override for table '{table_clean}': {configured_query}")
            return configured_query

        default_filter = config.get("default_delta_filter", "sys_updated_on>={last_load_date}")
        final_filter = default_filter.replace("{last_load_date}", last_load_date)
        logger.info(f"Using Default Delta Filter for table '{table_clean}': {final_filter}")
        return final_filter

    @classmethod
    def get_table_endpoint(
        cls,
        source_system: str,
        table_name: str,
        source_config: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Resolves the API endpoint path for a table (checking custom_table_endpoints first).
        """
        config = source_config or cls.get_source_config(source_system)
        table_clean = table_name.strip()

        custom_endpoints = config.get("custom_table_endpoints", {})
        if table_clean in custom_endpoints and custom_endpoints[table_clean]:
            logger.info(f"Using Custom Table Endpoint for '{table_clean}': {custom_endpoints[table_clean]}")
            return custom_endpoints[table_clean]

        template = config.get("api_endpoint_template", "/api/now/table/{table_name}")
        return template.format(table_name=table_clean)

    @classmethod
    def get_pipeline_defaults(cls, config_dict: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Retrieves pipeline default settings."""
        config = config_dict or cls._config_cache or cls.load_config()
        return config.get("pipeline_defaults", {})

    @classmethod
    def get_default_setting(cls, key: str, fallback_value: Any, config_dict: Optional[Dict[str, Any]] = None) -> Any:
        """Retrieves a specific setting from pipeline_defaults with fallback."""
        defaults = cls.get_pipeline_defaults(config_dict)
        return defaults.get(key, fallback_value)
