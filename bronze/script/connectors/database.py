"""
Relational Database Connector — AWS Glue Data Pipeline.

Supports: PostgreSQL, MySQL, MariaDB, SQLite.

Required Secrets Manager keys:
  db_type  : 'postgresql' | 'mysql' | 'mariadb' | 'sqlite'
  host     : Database hostname or IP (not required for sqlite)
  dbname   : Database name
  username : Database user (not required for sqlite)
  password : Database password (not required for sqlite)
  port     : Port number — REQUIRED (PostgreSQL: 5432, MySQL: 3306)

Required bronze_config.json keys (source_systems.<db_type>):
  db_type         : Same as Secrets Manager (used if 'db_type' absent from secret)
  query_template  : Optional SQL template with {table_name} and {query_filter} placeholders
  fetch_size      : Rows fetched per cursor batch (default: 10000)
"""

import logging
from typing import Any, Callable, Dict, List, Optional

from config_loader import ConfigLoader

logger = logging.getLogger(__name__)

_SUPPORTED_ENGINES = ('postgresql', 'postgres', 'mysql', 'mariadb', 'sqlite')


class DatabaseConnector:
    """
    Incremental database connector with cursor batch fetching and S3 chunk streaming.
    """

    @staticmethod
    def fetch_delta(
        last_load_date: str,
        secret_dict: Dict[str, Any],
        table_name: str,
        source_config: Dict[str, Any],
        custom_query: Optional[str] = None,
        on_chunk_callback: Optional[Callable[[List[Dict[str, Any]], int], None]] = None,
        s3_chunk_size: int = 10000,
    ) -> List[Dict[str, Any]]:
        """
        Extracts rows from a database table updated since last_load_date.
        """
        if not table_name or not table_name.strip():
            raise ValueError("Database connector: 'table_name' is required.")
        if not last_load_date or not last_load_date.strip():
            raise ValueError(
                f"Database connector: 'last_load_date' is required for table '{table_name}'."
            )

        config      = source_config or {}
        source_name = secret_dict.get('db_type') or config.get('db_type') or 'database'

        query_filter  = ConfigLoader.get_table_query_filter(
            source_name, table_name, last_load_date, custom_query, config,
            upper_bound=config.get('upper_bound')
        )
        query_template = config.get('query_template') or \
            "SELECT * FROM {table_name} WHERE {query_filter} ORDER BY updated_at ASC"
        sql_query = query_template.format(table_name=table_name, query_filter=query_filter)

        fetch_size = int(secret_dict.get('fetch_size') or config.get('fetch_size') or 10000)

        logger.info(f"[Database/{table_name}] Connecting to '{source_name}' | SQL: {sql_query}")

        conn   = DatabaseConnector._connect(secret_dict, config)
        cursor = conn.cursor()
        all_records:    List[Dict[str, Any]] = []
        records_buffer: List[Dict[str, Any]] = []
        total = 0
        part  = 1

        try:
            cursor.execute(sql_query)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []

            while True:
                rows = cursor.fetchmany(fetch_size)
                if not rows:
                    break

                batch  = [dict(zip(columns, row)) for row in rows]
                total += len(batch)
                logger.info(f"[Database/{table_name}] Batch: {len(batch)} rows | total: {total}")

                if on_chunk_callback:
                    records_buffer.extend(batch)
                    if len(records_buffer) >= s3_chunk_size:
                        on_chunk_callback(records_buffer, part)
                        records_buffer = []
                        part += 1
                else:
                    all_records.extend(batch)

            if on_chunk_callback and records_buffer:
                on_chunk_callback(records_buffer, part)

            logger.info(f"[Database/{table_name}] Done. Total: {total}")
            return all_records if not on_chunk_callback else []

        except Exception as err:
            logger.error(f"[Database/{table_name}] Extraction failed: {err}")
            raise
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def _connect(secret_dict: Dict[str, Any], source_config: Dict[str, Any]):
        """
        Establishes a database connection.

        All connection parameters are read from Secrets Manager.
        'port' is required — raise if missing (no default assumed).
        """
        db_type = (secret_dict.get('db_type') or source_config.get('db_type') or '').lower()
        if not db_type:
            raise ValueError(
                "Database connector: 'db_type' is required. "
                "Set 'db_type' in Secrets Manager or in bronze_config.json "
                f"(source_systems.<source>.db_type). Supported: {_SUPPORTED_ENGINES}."
            )
        if db_type not in _SUPPORTED_ENGINES:
            raise ValueError(
                f"Database connector: unsupported 'db_type': '{db_type}'. "
                f"Supported engines: {_SUPPORTED_ENGINES}."
            )

        if db_type == 'sqlite':
            dbname = secret_dict.get('dbname')
            if not dbname:
                raise ValueError(
                    "SQLite connection requires 'dbname' (file path) in Secrets Manager."
                )
            import sqlite3
            return sqlite3.connect(dbname)

        # All other engines require host, dbname, username, password, port
        host = secret_dict.get('host')
        if not host:
            raise ValueError(
                f"Database connector (db_type='{db_type}'): 'host' is required in Secrets Manager."
            )

        dbname = secret_dict.get('dbname')
        if not dbname:
            raise ValueError(
                f"Database connector (db_type='{db_type}'): 'dbname' is required in Secrets Manager."
            )

        username = secret_dict.get('username')
        if not username:
            raise ValueError(
                f"Database connector (db_type='{db_type}'): 'username' is required in Secrets Manager."
            )

        password = secret_dict.get('password', '')

        port_raw = secret_dict.get('port')
        if port_raw is None:
            raise ValueError(
                f"Database connector (db_type='{db_type}'): 'port' is required in Secrets Manager. "
                f"Standard ports — PostgreSQL: 5432, MySQL/MariaDB: 3306."
            )
        port = int(port_raw)

        if db_type in ('postgresql', 'postgres'):
            try:
                import pg8000.native as pg8000
                native = pg8000.Connection(
                    user=username, host=host, port=port, database=dbname, password=password
                )

                class _Adapter:
                    def __init__(self, c): self._c = c
                    def cursor(self): return self
                    def execute(self, q): self._r = self._c.run(q)
                    @property
                    def description(self):
                        return (
                            [(col['name'], col['type_oid'], None, None, None, None, None)
                             for col in self._r.columns]
                            if hasattr(self, '_r') and self._r.columns else []
                        )
                    def fetchmany(self, n):
                        if not hasattr(self, '_r') or not self._r.rows: return []
                        rows, self._r.rows = self._r.rows[:n], self._r.rows[n:]
                        return rows
                    def close(self): pass

                return _Adapter(native)
            except ImportError:
                import psycopg2
                return psycopg2.connect(
                    host=host, port=port, dbname=dbname, user=username, password=password
                )

        # mysql / mariadb
        import pymysql
        return pymysql.connect(
            host=host, port=port, database=dbname, user=username, password=password
        )
