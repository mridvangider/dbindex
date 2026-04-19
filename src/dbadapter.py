from abc import ABC, abstractmethod
from typing import List, Dict, Optional


class DBAdapter(ABC):
    """Abstract base class for database adapters implementing MCP server tools."""

    @abstractmethod
    def __init__(self, connection_params: dict):
        """Initialize adapter with credential parameters"""
        pass

    @abstractmethod
    def connect(self):
        """Establish database connection"""
        pass

    @abstractmethod
    def close(self):
        """Close database connection"""
        pass

    @abstractmethod
    def get_all_schemas(self) -> List[Dict]:
        """Get all available schemas."""
        pass

    @abstractmethod
    def get_all_tables(self, schema: Optional[str] = None) -> List[Dict]:
        """Get all tables in the database (or specific schema)."""
        pass

    @abstractmethod
    def get_all_columns(
        self, schema: Optional[str] = None, table: Optional[str] = None
    ) -> List[Dict]:
        """Get all columns from specified table(s)."""
        pass

    @abstractmethod
    def get_sample_data(
        self,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict]:
        """Get sample data from specified table(s)."""
        pass

    @abstractmethod
    def __enter__(self):
        """Entry point for context manager."""
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit point for context manager."""
        pass

    @abstractmethod
    def __execute_and_fetch(self, query: str, params: dict | tuple) -> list[dict]:
        """Runs a query and returns the results"""
        pass