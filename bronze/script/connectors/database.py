"""
Dynamic Database Connector for Relational Databases (PostgreSQL, MySQL, Oracle, SQL Server).

Strict Policy: No hardcoded default hostnames, database names, or table names.
Raises explicit ValueError if any required parameter is missing to prevent wrong data extraction.
"""

import logging
from typing import List, Dict, Any, Optional, Callable
from config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class DatabaseConnector:
    """
    Dynamic Database Connector for extracting incremental delta rows from RDBMS sources.
    Handles cursor batch fetching and streams chunks to S3 callbacks.
    """

    @staticmethod
    def fetch_delta(
        last_load_date: str,
        secret_dict: Dict[str, Any],
        table_name: str,
        source_config: Dict[str, Any],
        custom_query: Optional[str] = None,
        on_chunk_callback: Optional[Callable[[List[Dict[str, Any]], int], None]] = None,
        s3_chunk_size: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Extracts incremental rows from a database table modified/updated since last_load_date.
        Raises ValueError if required parameters or database connection details are missing.
        """
        if not table_name or not table_name.strip():
            raise ValueError("Database connector error: 'table_name' parameter is required and cannot be empty.")

        if not last_load_date or not last_load_date.strip():
            raise ValueError(f"Database connector error: 'last_load_date' is required for table '{table_name}'.")

        config = source_config or {}
        source_name = config.get('db_type') or secret_dict.get('engine') or 'database'
        
        query_filter = ConfigLoader.get_table_query_filter(source_name, table_name, last_load_date, custom_query, config)
        query_template = config.get('query_template') or "SELECT * FROM {table_name} WHERE {query_filter} ORDER BY updated_at ASC"
        
        sql_query = query_template.format(table_name=table_name, query_filter=query_filter)
        fetch_size = config.get('fetch_size') or secret_dict.get('fetch_size') or 10000
        fetch_size = int(fetch_size)

        logger.info(f"Connecting to database source '{source_name}' to extract table '{table_name}'...")
        logger.info(f"Executing SQL Query: {sql_query}")

        conn = DatabaseConnector._get_connection(secret_dict, config)
        cursor = conn.cursor()

        records_buffer = []
        all_records = []
        part_number = 1
        total_extracted = 0

        try:
            cursor.execute(sql_query)
            
            # Extract column names from cursor description
            columns = [desc[0] for desc in cursor.description] if cursor.description else []

            while True:
                rows = cursor.fetchmany(fetch_size)
                if not rows:
                    break

                batch = [dict(zip(columns, row)) for row in rows]
                batch_count = len(batch)
                total_extracted += batch_count

                logger.info(f"Database table '{table_name}': fetched batch of {batch_count} rows (Total: {total_extracted})")

                if on_chunk_callback:
                    records_buffer.extend(batch)
                    if len(records_buffer) >= s3_chunk_size:
                        logger.info(f"Chunk threshold reached ({len(records_buffer)} rows). Flushing part {part_number} to S3 STAGING...")
                        on_chunk_callback(records_buffer, part_number)
                        records_buffer = []
                        part_number += 1
                else:
                    all_records.extend(batch)

            if on_chunk_callback and records_buffer:
                logger.info(f"Flushing final part {part_number} ({len(records_buffer)} rows) to S3 STAGING...")
                on_chunk_callback(records_buffer, part_number)
                records_buffer = []

            logger.info(f"Finished database extraction for table '{table_name}'. Total records: {total_extracted}")
            return all_records if not on_chunk_callback else []

        except Exception as err:
            logger.error(f"Error during database extraction for table '{table_name}': {err}")
            raise
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def _get_connection(secret_dict: Dict[str, Any], source_config: Dict[str, Any]):
        """
        Dynamically establishes DB connection using driver libraries (pg8000, pymysql, psycopg2, sqlite3).
        Validates host, dbname, and credentials strictly.
        """
        db_type = (source_config.get('db_type') or secret_dict.get('engine') or '').lower()
        if not db_type:
            raise ValueError("Database connection error: 'db_type' or 'engine' must be specified in config or secret.")

        if db_type == 'sqlite':
            dbname = secret_dict.get('dbname') or secret_dict.get('database')
            if not dbname:
                raise ValueError("SQLite connection error: 'dbname' or 'database' path is required.")
            import sqlite3
            return sqlite3.connect(dbname)

        host = secret_dict.get('host') or secret_dict.get('hostname') or source_config.get('host')
        if not host:
            raise ValueError(f"Database connection error for '{db_type}': Database 'host' / 'hostname' is missing in Secret.")

        dbname = secret_dict.get('dbname') or secret_dict.get('database') or source_config.get('dbname')
        if not dbname:
            raise ValueError(f"Database connection error for '{db_type}': Database 'dbname' / 'database' is missing in Secret.")

        user = secret_dict.get('username') or secret_dict.get('user')
        if not user:
            raise ValueError(f"Database connection error for '{db_type}': Database 'username' / 'user' is missing in Secret.")

        password = secret_dict.get('password', '')
        port = int(secret_dict.get('port') or source_config.get('port') or (5432 if 'postgre' in db_type else 3306))

        if db_type in ('postgresql', 'postgres'):
            try:
                # pyrefly: ignore [missing-import]
                import pg8000.native
                conn = pg8000.native.Connection(user=user, host=host, port=port, database=dbname, password=password)
                class DBAPIAdapter:
                    def __init__(self, native_conn): self.native = native_conn
                    def cursor(self): return self
                    def execute(self, query): self.res = self.native.run(query)
                    @property
                    def description(self):
                        return [(col['name'], col['type_oid'], None, None, None, None, None) for col in self.res.columns] if hasattr(self, 'res') and self.res.columns else []
                    def fetchmany(self, size):
                        if not hasattr(self, 'res') or not self.res.rows: return []
                        rows = self.res.rows[:size]
                        self.res.rows = self.res.rows[size:]
                        return rows
                    def close(self): pass
                return DBAPIAdapter(conn)
            except ImportError:
                import psycopg2
                return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)

        elif db_type in ('mysql', 'mariadb'):
            import pymysql
            return pymysql.connect(host=host, port=port, database=dbname, user=user, password=password)

        else:
            raise ValueError(f"Unsupported database engine type '{db_type}'. Supported engines: postgresql, mysql, mariadb, sqlite.")
