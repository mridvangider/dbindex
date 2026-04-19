import os
import pytest
from exasol_adapter import ExasolAdapter


class TestExasolAdapter:
    """Unit tests for ExasolAdapter class."""

    def setup_method(self):
        """Set up test environment variables."""
        self.connection_params = {
            "dsn": os.getenv("EXASOL_DSN", "localhost"),
            "user": os.getenv("EXASOL_USER", "sys"),
            "password": os.getenv("EXASOL_PASSWORD", "exasol"),
            "encryption": os.getenv("EXASOL_SECURE", "false").lower() == "true",
        }

    def test_init(self):
        """Test adapter initialization."""
        adapter = ExasolAdapter(self.connection_params)
        assert adapter.connection_params == self.connection_params
        assert adapter._client is None

    def test_context_manager(self):
        """Test context manager entry and exit."""
        with ExasolAdapter(self.connection_params) as adapter:
            assert adapter._client is not None
            # Verify connection parameters are set correctly
            assert adapter.connection_params["fetch_dict"] is True
            assert adapter.connection_params["autocommit"] is False
        # After context, client should be closed
        assert adapter._client is None

    def test_get_all_schemas(self):
        """Test getting all schemas."""
        with ExasolAdapter(self.connection_params) as adapter:
            schemas = adapter.get_all_schemas()
            assert isinstance(schemas, list)
            # Each schema should be a dict with 'schema' key
            for schema in schemas:
                assert isinstance(schema, dict)
                assert "SCHEMA_NAME" in schema
                assert isinstance(schema["SCHEMA_NAME"], str)

    def test_get_all_tables(self):
        """Test getting all tables."""
        with ExasolAdapter(self.connection_params) as adapter:
            # Test without schema filter
            tables = adapter.get_all_tables()
            assert isinstance(tables, list)
            for table in tables:
                assert isinstance(table, dict)
                assert "TABLE_NAME" in table
                assert isinstance(table["TABLE_NAME"], str)

            # Test with schema filter (if we have a schema)
            schemas = adapter.get_all_schemas()
            if schemas:
                test_schema = "INLOOP"
                schema_tables = adapter.get_all_tables(schema=test_schema)
                assert isinstance(schema_tables, list)
                for table in schema_tables:
                    assert isinstance(table, dict)
                    assert "TABLE_NAME" in table

    def test_get_all_columns(self):
        """Test getting all columns."""
        with ExasolAdapter(self.connection_params) as adapter:
            # Test without filters
            columns = adapter.get_all_columns()
            assert isinstance(columns, list)
            for column in columns:
                assert isinstance(column, dict)
                assert "COLUMN_NAME" in column
                assert "COLUMN_TYPE" in column
                assert isinstance(column["COLUMN_NAME"], str)
                assert isinstance(column["COLUMN_TYPE"], str)

            # Test with schema filter
            schemas = adapter.get_all_schemas()
            test_schema = "INLOOP"
            if schemas:
                # test_schema = schemas[0]["schema"]
                schema_columns = adapter.get_all_columns(schema=test_schema)
                assert isinstance(schema_columns, list)
                for column in schema_columns:
                    assert isinstance(column, dict)
                    assert "COLUMN_NAME" in column
                    assert "COLUMN_TYPE" in column

            # Test with schema and table filters
            tables = adapter.get_all_tables(schema=test_schema)
            if tables:
                test_table = "INLOOP"
                table_columns = adapter.get_all_columns(
                    schema=test_schema, table=test_table
                )
                assert isinstance(table_columns, list)
                for column in table_columns:
                    assert isinstance(column, dict)
                    assert "COLUMN_NAME" in column
                    assert "COLUMN_TYPE" in column

    def test_get_sample_data(self):
        """Test getting sample data."""
        with ExasolAdapter(self.connection_params) as adapter:
            schemas = adapter.get_all_schemas()
            if not schemas:
                pytest.skip("No schemas available for testing")

            test_schema = "INLOOP"
            tables = adapter.get_all_tables(schema=test_schema)
            if not tables:
                pytest.skip("No tables available for testing")

            test_table = "TEST"

            # Test default limit
            sample = adapter.get_sample_data(table=test_table, schema=test_schema)
            assert isinstance(sample, list)
            # Each row should be a dict
            for row in sample:
                assert isinstance(row, dict)

            # Test custom limit
            sample_limited = adapter.get_sample_data(
                table=test_table, schema=test_schema, limit=5
            )
            assert isinstance(sample_limited, list)
            assert len(sample_limited) <= 5

