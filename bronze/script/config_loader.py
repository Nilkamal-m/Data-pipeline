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
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


class ConfigLoader:
    """
    Centralized Configuration Loader supporting custom tables, table-specific load dates, and query overrides.
    """
    _config_cache: Dict[str, Dict[str, Any]] = {}
    _latest_config: Optional[Dict[str, Any]] = None
    _cached_s3_path: Optional[str] = None
    _cached_s3_client: Optional[Any] = None

    @classmethod
    def clear_cache(cls) -> None:
        """Clears cached configuration (useful for testing or runtime environment switches)."""
        cls._config_cache.clear()
        cls._latest_config = None
        cls._cached_s3_path = None
        cls._cached_s3_client = None

    @classmethod
    def set_loaded_config(cls, config_dict: Dict[str, Any], env: Optional[str] = None) -> None:
        """Explicitly registers a loaded config into the cache to guarantee all subsequent calls reuse it."""
        effective_env = (env or 'dev').strip().lower()
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
        env: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Loads the centralized Bronze configuration JSON file and dynamically interpolates all {env} placeholders.
        Implements multi-tiered caching so any subsequent call in the same runtime immediately reuses the loaded config.
        """
        effective_env = (env or os.environ.get('ENV') or os.environ.get('ENVIRONMENT') or 'dev').strip().lower()

        # Remember S3 parameters if provided
        if config_s3_path and str(config_s3_path).strip():
            cls._cached_s3_path = str(config_s3_path).strip()
        if s3_client:
            cls._cached_s3_client = s3_client

        # 1. Exact match by cache key
        cache_key = f"{config_s3_path or 'local'}:{effective_env}"
        if cache_key in cls._config_cache:
            return cls._config_cache[cache_key]

        # 2. When called without explicit S3 path, reuse cached config for this specific env
        if not config_s3_path and effective_env in cls._config_cache:
            return cls._config_cache[effective_env]

        # 3. Determine effective S3 path and S3 client
        target_s3_path = config_s3_path or cls._cached_s3_path or os.environ.get('CONFIG_S3_PATH')
        target_client = s3_client or cls._cached_s3_client

        raw_config: Optional[Dict[str, Any]] = None

        if target_s3_path and str(target_s3_path).startswith("s3://"):
            if target_client is None:
                try:
                    import boto3
                    target_client = boto3.client('s3')
                    cls._cached_s3_client = target_client
                except Exception as b3_err:
                    logger.warning(f"Could not initialize boto3 S3 client: {b3_err}")

            if target_client:
                try:
                    path_parts = str(target_s3_path).replace("s3://", "").split("/", 1)
                    bucket_name, object_key = path_parts[0], path_parts[1]
                    logger.info(f"Loading Bronze configuration from S3: '{target_s3_path}'")
                    response = target_client.get_object(Bucket=bucket_name, Key=object_key)
                    content = response['Body'].read().decode('utf-8')
                    raw_config = json.loads(content)
                except Exception as err:
                    logger.warning(f"Failed to load config from S3 path '{target_s3_path}': {err}. Falling back to local config search.")

        # 4. Search exhaustive local paths
        if raw_config is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            cwd = os.getcwd()
            possible_paths = [
                os.path.join(current_dir, "config", "bronze_config.json"),
                os.path.join(current_dir, "bronze_config.json"),
                os.path.join(cwd, "bronze_config.json"),
                os.path.join(cwd, "config", "bronze_config.json"),
                os.path.join(cwd, "bronze", "script", "config", "bronze_config.json"),
                os.path.join("/tmp", "bronze_config.json"),
                os.path.join("/tmp", "config", "bronze_config.json"),
                "bronze/script/config/bronze_config.json",
                "glue_jobs/bronze/config/bronze_config.json",
                "config/bronze_config.json",
                "bronze_config.json"
            ]

            for path in possible_paths:
                if os.path.exists(path):
                    logger.info(f"Loading Bronze configuration from local file: '{path}' (environment: '{effective_env}')")
                    try:
                        with open(path, "r", encoding="utf-8") as f:
                            raw_config = json.load(f)
                            break
                    except Exception as read_err:
                        logger.warning(f"Failed reading local config at '{path}': {read_err}")

        # 5. Fallback: try standard data lake bucket path if running in Glue
        if raw_config is None:
            fallback_bucket = os.environ.get('BRONZE_BUCKET') or f"uax-datalake-{effective_env}-bucket"
            fallback_s3_path = f"s3://{fallback_bucket}/bronze/script/config/bronze_config.json"
            if target_client is None:
                try:
                    import boto3
                    target_client = boto3.client('s3')
                    cls._cached_s3_client = target_client
                except Exception:
                    pass
            if target_client:
                try:
                    logger.info(f"Attempting fallback S3 config lookup at '{fallback_s3_path}'...")
                    path_parts = fallback_s3_path.replace("s3://", "").split("/", 1)
                    response = target_client.get_object(Bucket=path_parts[0], Key=path_parts[1])
                    raw_config = json.loads(response['Body'].read().decode('utf-8'))
                    cls._cached_s3_path = fallback_s3_path
                except Exception:
                    pass

        if raw_config is None:
            if cls._latest_config is not None:
                logger.warning("Config not found at new location; reusing previously loaded configuration from memory.")
                return cls._latest_config
            logger.error("Bronze configuration file 'bronze_config.json' not found in S3 or local paths.")
            raise FileNotFoundError("Bronze configuration file 'bronze_config.json' not found.")

        # Interpolate {env} and {ENV} throughout the entire config tree
        interpolated = cls.interpolate_env(raw_config, effective_env)
        cls._config_cache[cache_key] = interpolated
        cls._config_cache[effective_env] = interpolated
        cls._latest_config = interpolated
        return interpolated

    @classmethod
    def get_source_config(
        cls,
        source_system: str,
        config_dict: Optional[Dict[str, Any]] = None,
        env: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Retrieves configuration for a specific source system.
        """
        config = config_dict or cls._latest_config or cls.load_config(env=env)
        sources = config.get("source_systems", {})
        source_key = source_system.strip().lower()

        if source_key not in sources:
            logger.warning(f"Source system '{source_system}' not found in configuration. Returning empty defaults.")
            return {}

        return sources[source_key]

    @classmethod
    def get_table_config(
        cls,
        source_system: str,
        table_name: str,
        source_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Retrieves the table configuration dictionary from source_systems.<source>.tables.<table_name>.
        Returns empty dict if table is not defined under 'tables'.
        """
        config = source_config or cls.get_source_config(source_system)
        table_clean = table_name.strip()
        tables = config.get("tables", {})
        for key in [table_clean, table_clean.replace('_', '-'), table_clean.replace('-', '_')]:
            if key in tables and isinstance(tables[key], dict):
                return tables[key]
        return {}

    @classmethod
    def get_source_tables(
        cls,
        source_system: str,
        source_config: Optional[Dict[str, Any]] = None
    ) -> List[str]:
        """
        Resolves the list of tables to process for a source system.
        Precedence:
        1. Keys under source_systems.<source>.tables
        2. source_systems.<source>.default_tables
        3. Keys under source_systems.<source>.table_initial_load_dates
        """
        config = source_config or cls.get_source_config(source_system)
        if "tables" in config and isinstance(config["tables"], dict) and config["tables"]:
            return list(config["tables"].keys())
        if "default_tables" in config and isinstance(config["default_tables"], list) and config["default_tables"]:
            return list(config["default_tables"])
        if "table_initial_load_dates" in config and isinstance(config["table_initial_load_dates"], dict) and config["table_initial_load_dates"]:
            return list(config["table_initial_load_dates"].keys())
        return []

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
        Checks source_systems.<source>.tables.<table_name>.initial_load_date first,
        then falls back to legacy table_initial_load_dates.
        Raises ValueError if no initial load date is defined (Fallback load dates strictly disabled).
        """
        table_clean = table_name.strip()

        # 1. Check CLI passed initial date override first
        if cli_initial_date and cli_initial_date.strip():
            logger.info(f"Using CLI passed initial_load_date override for table '{table_clean}': {cli_initial_date.strip()}")
            return cli_initial_date.strip()

        config = source_config or cls.get_source_config(source_system)

        # 2. Check tables.<table_name>.initial_load_date (Unified source.tables structure)
        table_cfg = cls.get_table_config(source_system, table_clean, source_config=config)
        if table_cfg.get("initial_load_date") and str(table_cfg["initial_load_date"]).strip():
            matched_date = str(table_cfg["initial_load_date"]).strip()
            logger.info(f"Using configured initial_load_date from tables.{table_clean} for table '{table_clean}': {matched_date}")
            return matched_date

        # 3. Check legacy table_initial_load_dates in bronze_config.json
        table_dates = config.get("table_initial_load_dates", {})
        matched_date = None
        for key in [table_clean, table_clean.replace('_', '-'), table_clean.replace('-', '_')]:
            if key in table_dates and table_dates[key] and str(table_dates[key]).strip():
                matched_date = str(table_dates[key]).strip()
                break
        if matched_date:
            logger.info(f"Using configured initial_load_date from legacy table_initial_load_dates for table '{table_clean}': {matched_date}")
            return matched_date

        # 4. Check global default_initial_load_date in bronze_config.json defaults
        global_default = cls.get_default_setting("default_initial_load_date", None, config_dict=cls._latest_config)
        if global_default and str(global_default).strip():
            logger.info(f"Using global initial_load_date for table '{table_clean}': {global_default.strip()}")
            return global_default.strip()

        # 5. Strict Enforcement: Throw error if load date is NULL/missing
        err_msg = (
            f"CRITICAL ERROR: No initial load date specified for table '{table_clean}' in source '{source_system}'. "
            f"Fallback load dates are disabled to prevent loading unwanted past records. "
            f"Please configure 'initial_load_date' under tables.{table_clean} in bronze_config.json or pass '--INITIAL_LOAD_DATE'."
        )
        logger.error(err_msg)
        raise ValueError(err_msg)

    @classmethod
    def get_table_upper_bound(
        cls,
        source_system: str,
        table_name: str,
        cli_upper_bound: Optional[str] = None,
        source_config: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Retrieves table-specific upper_bound configured in bronze_config.json or CLI.
        Resolution precedence:
        1. CLI passed --UPPER_BOUND override (if non-empty).
        2. Per-table 'tables.<table_name>.upper_bound' in source configuration.
        3. Per-table 'table_upper_bounds' in legacy source configuration.
        4. Source-level 'upper_bound' in source configuration.
        5. Global 'upper_bound' in pipeline_defaults.
        6. Returns None (open-ended extraction up to current run time).
        """
        table_clean = table_name.strip()

        # 1. Check CLI passed upper_bound override first
        if cli_upper_bound and str(cli_upper_bound).strip():
            logger.info(f"Using CLI passed upper_bound override for table '{table_clean}': {str(cli_upper_bound).strip()}")
            return str(cli_upper_bound).strip()

        config = source_config or cls.get_source_config(source_system)

        # 2. Check tables.<table_name>.upper_bound (Unified source.tables structure)
        table_cfg = cls.get_table_config(source_system, table_clean, source_config=config)
        if table_cfg.get("upper_bound") and str(table_cfg["upper_bound"]).strip():
            matched_ub = str(table_cfg["upper_bound"]).strip()
            logger.info(f"Using table-specific upper_bound from tables.{table_clean} for table '{table_clean}': {matched_ub}")
            return matched_ub

        # 3. Check legacy table_upper_bounds in bronze_config.json
        table_upper_bounds = config.get("table_upper_bounds", {})
        matched_ub = None
        for key in [table_clean, table_clean.replace('_', '-'), table_clean.replace('-', '_')]:
            if key in table_upper_bounds and table_upper_bounds[key] and str(table_upper_bounds[key]).strip():
                matched_ub = str(table_upper_bounds[key]).strip()
                break
        if matched_ub:
            logger.info(f"Using table-specific upper_bound from legacy table_upper_bounds for table '{table_clean}': {matched_ub}")
            return matched_ub

        # 4. Check source-level upper_bound in bronze_config.json
        source_ub = config.get("upper_bound")
        if source_ub and str(source_ub).strip():
            logger.info(f"Using source-level upper_bound for table '{table_clean}': {str(source_ub).strip()}")
            return str(source_ub).strip()

        # 5. Check global pipeline_defaults.upper_bound in bronze_config.json
        global_ub = cls.get_default_setting("upper_bound", None, config_dict=cls._latest_config)
        if global_ub and str(global_ub).strip():
            logger.info(f"Using global pipeline_defaults upper_bound for table '{table_clean}': {str(global_ub).strip()}")
            return str(global_ub).strip()

        return None

    @classmethod
    def get_table_query_filter(
        cls,
        source_system: str,
        table_name: str,
        last_load_date: str,
        custom_query_cli: Optional[str] = None,
        source_config: Optional[Dict[str, Any]] = None,
        upper_bound: Optional[str] = None,
    ) -> str:
        """
        Resolves the final OData query filter for a table.

        For Moveworks, builds a bounded 'ge ... le' filter when upper_bound is
        provided (parallel shard mode), or an open-ended 'ge' filter when
        upper_bound is None (sequential mode).  Both are backward-compatible
        with the existing call from uax_bronze_load.py which does not pass
        upper_bound.

        Args:
            source_system:     Source system key (e.g. 'moveworks', 'servicenow').
            table_name:        Entity / table name.
            last_load_date:    Watermark lower-bound timestamp (inclusive).
            custom_query_cli:  Optional OData filter override from CLI.
            source_config:     Source-system config block from bronze_config.json.
            upper_bound:       Inclusive upper-bound timestamp for shard windows.
                               None = open-ended (non-parallel / sequential mode).
        """
        config = source_config or cls.get_source_config(source_system)
        table_clean = table_name.strip()

        effective_ub = (
            str(upper_bound).strip()
            if upper_bound and str(upper_bound).strip()
            else datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        )

        # For Moveworks OData v4, timestamps in $filter must be ISO-8601 UTC (e.g. 'YYYY-MM-DDTHH:MM:SSZ')
        effective_lb = last_load_date
        if source_system.strip().lower() == 'moveworks':
            def _to_iso(ts_str: str) -> str:
                s = str(ts_str).strip()
                try:
                    clean = s
                    if clean.endswith('Z') or clean.endswith('z'):
                        clean = clean[:-1] + '+00:00'
                    elif ' ' in clean and '+' not in clean and '-' not in clean[10:]:
                        clean = clean.replace(' ', 'T') + '+00:00'
                    elif 'T' in clean and '+' not in clean and '-' not in clean[10:]:
                        clean = clean + '+00:00'
                    dt = datetime.fromisoformat(clean)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                except Exception:
                    return s.replace(' ', 'T')
            effective_ub = _to_iso(effective_ub)
            effective_lb = _to_iso(last_load_date)

        if custom_query_cli and custom_query_cli.strip():
            cli_query = custom_query_cli.strip()
            if "sys_updated_on" not in cli_query and "updated_at" not in cli_query and "last_updated_time" not in cli_query:
                if source_system == 'servicenow':
                    delta_part = f"sys_updated_on>={effective_lb}"
                elif source_system in ['postgresql', 'mysql']:
                    delta_part = f"updated_at >= '{effective_lb}' and updated_at <= '{effective_ub}'"
                elif source_system == 'moveworks':
                    delta_part = f"last_updated_time ge '{effective_lb}' and last_updated_time le '{effective_ub}'"
                else:
                    delta_part = f"{effective_lb}"

                separator = "^" if source_system == 'servicenow' else (" and " if source_system == 'moveworks' else " AND ")
                final_filter = f"{cli_query}{separator}{delta_part}"
            else:
                final_filter = (
                    cli_query
                    .replace("{last_load_date}", effective_lb)
                    .replace("{upper_bound}", effective_ub)
                )
            logger.info(f"Using CLI Custom Query override for table '{table_clean}': {final_filter}")
            return final_filter

        # 1. Check tables.<table_name>.query_override (Unified source.tables structure)
        table_cfg = cls.get_table_config(source_system, table_clean, source_config=config)
        if table_cfg.get("query_override") and str(table_cfg["query_override"]).strip():
            configured_query = (
                str(table_cfg["query_override"]).strip()
                .replace("{last_load_date}", effective_lb)
                .replace("{upper_bound}", effective_ub)
            )
            logger.info(f"Using Configured Table Query override from tables.{table_clean} for table '{table_clean}': {configured_query}")
            return configured_query

        # 2. Check legacy table_query_overrides
        table_overrides = config.get("table_query_overrides", {})
        for key in [table_clean, table_clean.replace('_', '-'), table_clean.replace('-', '_')]:
            if key in table_overrides and table_overrides[key]:
                configured_query = (
                    table_overrides[key]
                    .replace("{last_load_date}", effective_lb)
                    .replace("{upper_bound}", effective_ub)
                )
                logger.info(f"Using Configured Table Query override from legacy table_query_overrides for table '{table_clean}': {configured_query}")
                return configured_query

        # Moveworks full initial load: when last_load_date is 1900 or 1970 and upper_bound is open-ended,
        # omit $filter to allow Moveworks to return all records desde inception without date restriction.
        if source_system.strip().lower() == 'moveworks' and (str(last_load_date).strip().startswith('1900') or str(last_load_date).strip().startswith('1970')):
            is_open_ub = not upper_bound or str(upper_bound).strip() == "" or str(upper_bound).strip().startswith('9999') or str(upper_bound).strip().startswith('9998')
            if is_open_ub:
                logger.info(
                    f"Moveworks table '{table_clean}': Full initial load detected (last_load_date='{last_load_date}'). "
                    f"OData $filter omitted for 100% complete historical extraction."
                )
                return ""
            else:
                final_filter = f"last_updated_time le '{effective_ub}'"
                logger.info(f"Moveworks table '{table_clean}': Historical backfill filter: {final_filter}")
                return final_filter

        default_filter = config.get("default_delta_filter", "sys_updated_on>={last_load_date}")
        final_filter = (
            default_filter
            .replace("{last_load_date}", effective_lb)
            .replace("{upper_bound}", effective_ub)
        )
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
        Resolves the API endpoint path for a table.
        Checks tables.<table_name>.custom_endpoint first, then legacy custom_table_endpoints.
        """
        config = source_config or cls.get_source_config(source_system)
        table_clean = table_name.strip()

        # 1. Check tables.<table_name>.custom_endpoint (Unified source.tables structure)
        table_cfg = cls.get_table_config(source_system, table_clean, source_config=config)
        if table_cfg.get("custom_endpoint") and str(table_cfg["custom_endpoint"]).strip():
            logger.info(f"Using Custom Table Endpoint from tables.{table_clean} for '{table_clean}': {table_cfg['custom_endpoint']}")
            return str(table_cfg["custom_endpoint"]).strip()

        # 2. Check legacy custom_table_endpoints
        custom_endpoints = config.get("custom_table_endpoints", {})
        for key in [table_clean, table_clean.replace('_', '-'), table_clean.replace('-', '_')]:
            if key in custom_endpoints and custom_endpoints[key]:
                logger.info(f"Using Custom Table Endpoint from legacy custom_table_endpoints for '{table_clean}': {custom_endpoints[key]}")
                return custom_endpoints[key]

        template = config.get("api_endpoint_template", "/api/now/table/{table_name}")
        return template.format(table_name=table_clean)

    @classmethod
    def get_table_file_path(
        cls,
        source_system: str,
        table_name: str,
        source_config: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Resolves the S3 file path / prefix for a table.
        Checks tables.<table_name>.file_path first, then legacy table_paths.
        """
        config = source_config or cls.get_source_config(source_system)
        table_clean = table_name.strip()

        # 1. Check tables.<table_name>.file_path (Unified source.tables structure)
        table_cfg = cls.get_table_config(source_system, table_clean, source_config=config)
        if table_cfg.get("file_path") and str(table_cfg["file_path"]).strip():
            return str(table_cfg["file_path"]).strip()

        # 2. Check legacy table_paths
        table_paths = config.get("table_paths", {})
        for key in [table_clean, table_clean.replace('_', '-'), table_clean.replace('-', '_')]:
            if key in table_paths and table_paths[key]:
                return str(table_paths[key]).strip()

        return None

    @classmethod
    def get_table_fetch_mode(
        cls,
        source_system: str,
        table_name: str,
        source_config: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Resolves the fetch mode ('all' or 'latest') for an S3 file table.
        Checks tables.<table_name>.fetch_mode first, then legacy table_fetch_modes.
        """
        config = source_config or cls.get_source_config(source_system)
        table_clean = table_name.strip()

        # 1. Check tables.<table_name>.fetch_mode (Unified source.tables structure)
        table_cfg = cls.get_table_config(source_system, table_clean, source_config=config)
        if table_cfg.get("fetch_mode") and str(table_cfg["fetch_mode"]).strip():
            return str(table_cfg["fetch_mode"]).strip().lower()

        # 2. Check legacy table_fetch_modes
        table_modes = config.get("table_fetch_modes", {})
        for key in [table_clean, table_clean.replace('_', '-'), table_clean.replace('-', '_')]:
            if key in table_modes and table_modes[key]:
                return str(table_modes[key]).strip().lower()

        return None

    @classmethod
    def get_pipeline_defaults(cls, config_dict: Optional[Dict[str, Any]] = None, env: Optional[str] = None) -> Dict[str, Any]:
        """Retrieves pipeline default settings."""
        if config_dict and "pipeline_defaults" in config_dict:
            return config_dict.get("pipeline_defaults", {})
        config = config_dict or cls._latest_config or cls.load_config(env=env)
        return config.get("pipeline_defaults", {})

    @classmethod
    def get_default_setting(
        cls,
        key: str,
        fallback_value: Any,
        config_dict: Optional[Dict[str, Any]] = None,
        env: Optional[str] = None
    ) -> Any:
        """Retrieves a specific setting from pipeline_defaults with fallback."""
        cfg = config_dict or cls._latest_config
        defaults = cls.get_pipeline_defaults(cfg, env=env)
        return defaults.get(key, fallback_value)

    @classmethod
    def get_glue_catalog_config(cls, config_dict: Optional[Dict[str, Any]] = None, env: Optional[str] = None) -> Dict[str, Any]:
        """Retrieves glue_catalog configuration from pipeline_defaults."""
        cfg = config_dict or cls._latest_config
        defaults = cls.get_pipeline_defaults(cfg, env=env)
        return defaults.get("glue_catalog", {})

    @classmethod
    def get_table_prefix(cls, config_dict: Optional[Dict[str, Any]] = None, env: Optional[str] = None) -> str:
        """
        Retrieves the centralized table_prefix configured under glue_catalog.
        Raises ValueError if table_prefix is missing or empty.
        """
        catalog_cfg = cls.get_glue_catalog_config(config_dict, env=env)
        prefix = catalog_cfg.get("table_prefix")
        if not prefix or not str(prefix).strip():
            raise ValueError(
                "CRITICAL CONFIG ERROR: 'table_prefix' is missing or empty in bronze_config.json "
                "(pipeline_defaults.glue_catalog.table_prefix). Please configure it (e.g. 'raw_tbl_')."
            )
        return str(prefix).strip()

    @classmethod
    def get_glue_database(cls, config_dict: Optional[Dict[str, Any]] = None, env: Optional[str] = None) -> str:
        """
        Retrieves the centralized database_name configured under glue_catalog.
        Dynamically interpolates {env} if present.
        Raises ValueError if database_name is missing or empty.
        """
        catalog_cfg = cls.get_glue_catalog_config(config_dict, env=env)
        db = catalog_cfg.get("database_name")
        if not db or not str(db).strip():
            raise ValueError(
                "CRITICAL CONFIG ERROR: 'database_name' is missing or empty in bronze_config.json "
                "(pipeline_defaults.glue_catalog.database_name). Please configure it (e.g. 'uax_datalake_db_{env}')."
            )
        db_str = str(db).strip()
        if "{env}" in db_str or "{ENV}" in db_str:
            effective_env = (env or os.environ.get('ENV') or os.environ.get('ENVIRONMENT') or 'dev').strip().lower()
            db_str = db_str.replace("{env}", effective_env).replace("{ENV}", effective_env.upper())
        return db_str

    @classmethod
    def get_crawler_name(cls, config_dict: Optional[Dict[str, Any]] = None, env: Optional[str] = None) -> Optional[str]:
        """
        Retrieves the centralized crawler_name configured under glue_catalog.
        Dynamically interpolates {env} if present.
        """
        catalog_cfg = cls.get_glue_catalog_config(config_dict, env=env)
        crawler = catalog_cfg.get("crawler_name")
        if not crawler or not str(crawler).strip():
            return None
        c_str = str(crawler).strip()
        if "{env}" in c_str or "{ENV}" in c_str:
            effective_env = (env or os.environ.get('ENV') or os.environ.get('ENVIRONMENT') or 'dev').strip().lower()
            c_str = c_str.replace("{env}", effective_env).replace("{ENV}", effective_env.upper())
        return c_str

    @classmethod
    def get_watermark_table_name(cls, config_dict: Optional[Dict[str, Any]] = None, env: Optional[str] = None) -> str:
        """
        Retrieves the centralized watermark_table_name configured under glue_catalog.
        Raises ValueError if watermark_table_name is missing or empty.
        """
        catalog_cfg = cls.get_glue_catalog_config(config_dict, env=env)
        wm = catalog_cfg.get("watermark_table_name")
        if not wm or not str(wm).strip():
            raise ValueError(
                "CRITICAL CONFIG ERROR: 'watermark_table_name' is missing or empty in bronze_config.json "
                "(pipeline_defaults.glue_catalog.watermark_table_name). Please configure it (e.g. 'raw_tbl_watermarks')."
            )
        return str(wm).strip()
