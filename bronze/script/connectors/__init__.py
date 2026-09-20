"""
Connector registry — AWS Glue Data Pipeline.

Maps source system names to their connector classes.
All connectors expose a single public method: fetch_delta().
"""

from .servicenow import ServiceNowConnector
from .genesys    import GenesysConnector
from .moveworks  import MoveworksConnector
from .database   import DatabaseConnector
from .s3_file    import S3FileConnector
from .oauth      import OAuth2Client

# Canonical source system → connector class mapping.
# Add new source systems here only — no other file needs to change.
CONNECTOR_MAP = {
    'servicenow':  ServiceNowConnector,
    'genesys':     GenesysConnector,
    'moveworks':   MoveworksConnector,
    'postgresql':  DatabaseConnector,
    'mysql':       DatabaseConnector,
    'mariadb':     DatabaseConnector,
    'sqlite':      DatabaseConnector,
    's3_file':     S3FileConnector,
}


def get_connector(source_system: str, source_config: dict = None):
    """
    Returns the connector class for a given source system name.

    Lookup order:
      1. Exact key match in CONNECTOR_MAP  (e.g. 'moveworks', 's3_file').
      2. Fallback to source_config['type'] (for named vendor sources such as
         'vendor_a_s3' with config.type = 's3_file').

    Args:
        source_system:  Canonical source system key (e.g. 'moveworks', 'vendor_a_s3').
        source_config:  Optional source config block from bronze_config.json.
                        Required for vendor sources not directly in CONNECTOR_MAP.

    Raises:
        ValueError: If the source system is not registered and 'type' is absent or unknown.
    """
    key = source_system.strip().lower()

    # 1. Exact match
    connector = CONNECTOR_MAP.get(key)
    if connector is not None:
        return connector

    # 2. Type-based routing for vendor/custom sources (e.g. vendor_a_s3 with type='s3_file')
    if source_config:
        type_key = str(source_config.get('type', '')).strip().lower()
        connector = CONNECTOR_MAP.get(type_key)
        if connector is not None:
            return connector

    supported = ', '.join(sorted(CONNECTOR_MAP.keys()))
    raise ValueError(
        f"Unsupported source system: '{source_system}'. "
        f"Registered connectors: {supported}. "
        "For custom vendor sources, add \"type\": \"s3_file\" (or other type) to "
        "bronze_config.json, or register the source in connectors/__init__.py."
    )


__all__ = [
    'ServiceNowConnector',
    'GenesysConnector',
    'MoveworksConnector',
    'DatabaseConnector',
    'S3FileConnector',
    'OAuth2Client',
    'CONNECTOR_MAP',
    'get_connector',
]
