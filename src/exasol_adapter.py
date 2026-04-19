from typing import List, Dict, Optional, cast
import pyexasol


class ExasolAdapter:
    """Exasol database adapter using pyexasol client."""

    def __init__(self, connection_params: dict):
        """Initialize Exasol connection.

        Args:
            connection_params: Dictionary containing host, port, user, password, secure
        """
        self.connection_params = connection_params
        self.connection_params["fetch_dict"] = True
        self.connection_params["auto_commit"] = False
        self._client = None

    def connect(self):
        """Establish database connection."""
        self._connect()

    def close(self):
        """Close database connection."""
        self._disconnect()

    def _connect(self):
        """Establish database connection."""
        if self._client is None:
            self._client = pyexasol.connect(**self.connection_params)

    def _disconnect(self):
        """Close database connection."""
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            finally:
                self._client = None

    def get_all_schemas(self) -> List[Dict]:
        """Get all available schemas."""
        result = self.__execute_and_fetch("SHOW SCHEMAS")
        return [{"schema": row["SCHEMA_NAME"]} for row in result]

    def get_all_tables(self, schema: Optional[str] = None) -> List[Dict]:
        """Get all tables in the database (or specific schema)."""
        result = self.__execute_and_fetch(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        )
        if schema:
            return [{"table": row["TABLE_NAME"]} for row in result if row["TABLE_SCHEMA"] == schema]
        return [{"table": row["TABLE_NAME"]} for row in result]

    def get_all_columns(
        self, schema: Optional[str] = None, table: Optional[str] = None
    ) -> List[Dict]:
        """Get all columns from specified table(s)."""
        if schema and table:
            query = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' ORDER BY ORDINAL_POSITION"
        elif schema:
            query = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' ORDER BY TABLE_NAME, ORDINAL_POSITION"
        else:
            query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS ORDER BY TABLE_NAME, ORDINAL_POSITION"

        result = self.__execute_and_fetch(query)
        return [
            {"column": row["COLUMN_NAME"], "data_type": row["DATA_TYPE"]} for row in result
        ]

    def __execute_and_fetch(
        self, query: str, params: dict | tuple | None = None
    ) -> list[dict]:
        """Execute a query and return results as list of dictionaries."""
        if self._client is None:
            raise RuntimeError("Client not connected. Use context manager first.")

        with self._client.execute(query) as s:
            rows = cast(list[dict], s.fetchall())
            return rows

    def get_sample_data(
        self,
        table: str,
        schema: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict]:
        """Get sample data from specified table(s)."""
        schema_str = f"{schema}." if schema else ""
        query = f"SELECT * FROM {schema_str}{table} LIMIT {limit}"
        
        result = self.__execute_and_fetch(query)
        return result

    def __enter__(self):
        """Entry point for context manager."""
        self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit point for context manager."""
        self._disconnect()
        return False
