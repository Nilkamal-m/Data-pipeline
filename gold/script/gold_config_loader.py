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
    _config_cache: Optional[Dict[str, Any]] = None

    @classmethod
    def load_config(cls, config_s3_path: Optional[str] = None, s3_client: Optional[Any] = None) -> Dict[str, Any]:
        """
        Loads the Gold configuration JSON file from S3 or local disk.
        Cached in memory after first load.
        """
        if cls._config_cache is not None:
            return cls._config_cache

        # 1. Try S3 path if provided
        if config_s3_path and config_s3_path.startswith("s3://") and s3_client:
            try:
                path_parts = config_s3_path.replace("s3://", "").split("/", 1)
                bucket_name, object_key = path_parts[0], path_parts[1]
                logger.info(f"Loading Gold configuration from S3: '{config_s3_path}'")
                response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
                content = response['Body'].read().decode('utf-8')
                cls._config_cache = json.loads(content)
                return cls._config_cache
            except Exception as err:
                logger.warning(f"Failed to load Gold config from S3 path '{config_s3_path}': {err}. Falling back to local search.")

        # 2. Local search candidates (handles local repo, AWS Glue /tmp, and relative execution paths)
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
                    cls._config_cache = json.load(f)
                    return cls._config_cache

        logger.warning("Gold configuration file 'gold_config.json' not found. Using empty defaults.")
        return {}

    @classmethod
    def clear_cache(cls) -> None:
        """Clears the cached configuration."""
        cls._config_cache = None

    @classmethod
    def get_defaults(cls, config_dict: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Retrieves pipeline defaults block."""
        config = config_dict if config_dict is not None else (cls._config_cache or cls.load_config())
        return config.get("pipeline_defaults", {})

    @classmethod
    def get_source_config(cls, source_system: str, config_dict: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Retrieves configuration for a specific source system in Gold layer."""
        config = config_dict if config_dict is not None else (cls._config_cache or cls.load_config())
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
        Default standard convention: gold_<source>_<tablename>
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

        # Default standard: gold_<source>_<tablename>
        return f"gold_{clean_source}_{clean_table}"
