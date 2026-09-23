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
        env: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Loads the Gold configuration JSON file from S3 or local disk.
        Dynamically interpolates all {env} placeholders according to effective_env.
        Cached in memory per environment after first load.
        """
        if cls._config_cache is None or not isinstance(cls._config_cache, dict):
            cls._config_cache = {}
        effective_env = (env or os.environ.get('ENV') or os.environ.get('ENVIRONMENT') or 'dev').strip().lower()
        if effective_env in cls._config_cache:
            return cls._config_cache[effective_env]

        raw_config = None

        if config_s3_path and str(config_s3_path).strip():
            cls._cached_s3_path = str(config_s3_path).strip()
        if s3_client is not None:
            cls._cached_s3_client = s3_client

        active_s3_path = config_s3_path or cls._cached_s3_path
        active_s3_client = s3_client or cls._cached_s3_client

        # 1. Try S3 path if provided
        if active_s3_path and active_s3_path.startswith("s3://") and active_s3_client:
            try:
                path_parts = active_s3_path.replace("s3://", "").split("/", 1)
                bucket_name, object_key = path_parts[0], path_parts[1]
                logger.info(f"Loading Gold configuration from S3: '{active_s3_path}'")
                response = active_s3_client.get_object(Bucket=bucket_name, Key=object_key)
                content = response['Body'].read().decode('utf-8')
                raw_config = json.loads(content)
            except Exception as err:
                logger.warning(f"Failed to load Gold config from S3 path '{active_s3_path}': {err}. Falling back to local search.")

        # 2. Local search candidates (handles local repo, AWS Glue /tmp, and relative execution paths)
        if raw_config is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            possible_paths = [
                os.path.join(current_dir, "config", "gold_config.json"),
                "gold/script/config/gold_config.json",
                "scripts/gold/config/gold_config.json",
                "/tmp/gold_config.json"
            ]

            for path in possible_paths:
                if os.path.exists(path):
                    logger.info(f"Loading Gold configuration from local file: '{path}'")
                    with open(path, "r", encoding="utf-8") as f:
                        raw_config = json.load(f)
                        break

        if raw_config is None:
            logger.warning("Gold configuration file 'gold_config.json' not found. Using empty defaults.")
            raw_config = {}

        interpolated = cls.interpolate_env(raw_config, effective_env)
        cls._config_cache[effective_env] = interpolated
        cls._latest_config = interpolated
        return interpolated

    @classmethod
    def get_defaults(cls, config_dict: Optional[Dict[str, Any]] = None, env: Optional[str] = None) -> Dict[str, Any]:
        """Retrieves pipeline defaults block."""
        config = config_dict if config_dict is not None else ((cls._config_cache.get(env) if env else cls._latest_config) or cls.load_config(env=env))
        return config.get("pipeline_defaults", {})

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
        sources = config.get("source_systems", {})
        return sources.get(source_system.strip().lower(), {})

    @classmethod
    def get_table_config(cls, source_system: str, table_name: str, config_dict: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Retrieves table-specific configuration (primary_key, target_engines, transforms, etc.).
        Supports lookup by clean table name, 'v_<name>', or 'gold_<source>_<name>'.
        """
        source_cfg = cls.get_source_config(source_system, config_dict)
        table_configs = source_cfg.get("tables", {})
        clean = table_name.strip().lower()

        base = clean
        # Strip known prefixes
        for prefix in [f"gold_{source_system}_", "gold_tbl_", "gold_", "v_"]:
            if base.startswith(prefix):
                base = base[len(prefix):]
                break

        candidates = [clean, base, base.replace('-', '_'), base.replace('_', '-')]
        for cand in candidates:
            if cand in table_configs:
                return dict(table_configs[cand])

        return {}

    @classmethod
    def get_primary_key(cls, source_system: str, table_name: str, config_dict: Optional[Dict[str, Any]] = None) -> List[str]:
        """
        Retrieves primary key list for table upsert / merge.
        Returns empty list if not specified.
        """
        tbl_cfg = cls.get_table_config(source_system, table_name, config_dict)
        pk = tbl_cfg.get("primary_key") or tbl_cfg.get("natural_key") or tbl_cfg.get("nkey")
        if isinstance(pk, list):
            return [str(k).strip() for k in pk if str(k).strip()]
        elif isinstance(pk, str) and pk.strip():
            return [str(k).strip() for k in pk.split(',') if str(k).strip()]
        return []

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
