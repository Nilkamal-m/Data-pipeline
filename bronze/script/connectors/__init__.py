from .servicenow import ServiceNowConnector
from .genesys import GenesysConnector
from .moveworks import MoveworksConnector
from .database import DatabaseConnector
from .s3_file import S3FileConnector
from .oauth import OAuth2Client

CONNECTOR_MAP = {
    'servicenow': ServiceNowConnector,
    'genesys': GenesysConnector,
    'moveworks': MoveworksConnector,
    'database': DatabaseConnector,
    'postgresql': DatabaseConnector,
    'mysql': DatabaseConnector,
    'oracle': DatabaseConnector,
    'sqlserver': DatabaseConnector,
    's3_file': S3FileConnector,
    's3_bucket': S3FileConnector,
    's3_source': S3FileConnector
}

def get_connector(source_system: str, source_config: dict = None):
    """
    Factory function to retrieve the appropriate source connector instance or class.
    Checks CONNECTOR_MAP first, or inspects 'type' field in source_config.
    """
    source_key = source_system.lower()
    if source_key in CONNECTOR_MAP:
        return CONNECTOR_MAP[source_key]

    if source_config and isinstance(source_config, dict):
        connector_type = (source_config.get('type') or source_config.get('connector_type') or '').lower()
        if connector_type in CONNECTOR_MAP:
            return CONNECTOR_MAP[connector_type]

    supported = ", ".join(CONNECTOR_MAP.keys())
    raise ValueError(f"Unsupported source system: '{source_system}'. Allowed values/types: {supported}")

__all__ = [
    'ServiceNowConnector',
    'GenesysConnector',
    'MoveworksConnector',
    'DatabaseConnector',
    'S3FileConnector',
    'OAuth2Client',
    'CONNECTOR_MAP',
    'get_connector'
]
