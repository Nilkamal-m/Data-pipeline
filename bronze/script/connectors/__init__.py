from .servicenow import ServiceNowConnector
from .genesys import GenesysConnector
from .moveworks import MoveworksConnector
from .database import DatabaseConnector
from .oauth import OAuth2Client

CONNECTOR_MAP = {
    'servicenow': ServiceNowConnector,
    'genesys': GenesysConnector,
    'moveworks': MoveworksConnector,
    'database': DatabaseConnector,
    'postgresql': DatabaseConnector,
    'mysql': DatabaseConnector,
    'oracle': DatabaseConnector,
    'sqlserver': DatabaseConnector
}

def get_connector(source_system: str):
    """
    Factory function to retrieve the appropriate source connector instance or class.
    
    Args:
        source_system (str): Lowercase identifier of the source system.

    Returns:
        Connector class matching source_system.
        
    Raises:
        ValueError: If source_system is not supported.
    """
    source_key = source_system.lower()
    if source_key not in CONNECTOR_MAP:
        supported = ", ".join(CONNECTOR_MAP.keys())
        raise ValueError(f"Unsupported source system: '{source_system}'. Allowed values: {supported}")
    return CONNECTOR_MAP[source_key]

__all__ = [
    'ServiceNowConnector',
    'GenesysConnector',
    'MoveworksConnector',
    'DatabaseConnector',
    'OAuth2Client',
    'CONNECTOR_MAP',
    'get_connector'
]
