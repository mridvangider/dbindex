from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from typing import Optional
import json
import sys

from mcp.server.fastmcp import Context, FastMCP
from mcp.server.session import ServerSession

from app_context import AppContext, AppConfig


def load_config(path: str) -> AppConfig:
    with open(path, "r") as f:
        _config = json.load(f)
        config = AppConfig.model_validate_json(_config)
        return config

@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    config_path = sys.argv[1]
    config = load_config(config_path)
    with AppContext(config) as context:
        yield context

app = FastMCP(lifespan=app_lifespan)


# Access type-safe lifespan context in tools
@app.tool()
def get_all_schemas(ctx: Context[ServerSession, AppContext]) -> list[dict]:
    """Get all available schemas from the database.
    """
    adapter = ctx.request_context.lifespan_context.get_adapter()
    return adapter.get_all_schemas()


@app.tool()
def get_all_tables(
    ctx: Context[ServerSession, AppContext], schema: Optional[str] = None
) -> list[dict]:
    """Get all tables in the database (or specific schema).

    Args:
        schema: Optional schema name to filter tables. If None, returns tables from all schemas.
    """
    adapter = ctx.request_context.lifespan_context.get_adapter()
    return adapter.get_all_tables(schema)


@app.tool()
def get_all_columns(
    ctx: Context[ServerSession, AppContext], schema: Optional[str] = None, table: Optional[str] = None
) -> list[dict]:
    """Get all columns from specified table(s).

    Args:
        schema: Optional schema name to filter tables. If None, returns columns from all schemas and tables.
        table: Optional table name to filter columns. If None, returns columns from all tables in the schema or database.
    """
    adapter = ctx.request_context.lifespan_context.get_adapter()
    return adapter.get_all_columns(schema, table)

@app.tool()
def is_sample_data_enabled(ctx: Context[ServerSession, AppContext]) -> bool:
    """Returns whether the mcp tool `get_sample_data` is enabled or not"""
    return ctx.request_context.lifespan_context.config.enable_sample_data


@app.tool()
def get_sample_data(
    ctx: Context[ServerSession, AppContext],
    table: str,
    schema: Optional[str] = None,
    limit: int = 10
) -> list[dict]:
    """Get sample data from specified table(s). This function might be disabled by the mcp server config.
    In that case, this function returns empty table

    Args:
        table: Table name to get sample data from.
        schema: Optional schema name for the table. If None, searches all schemas.
        limit: Maximum number of rows to return. Default is 10.
    """
    config = ctx.request_context.lifespan_context.config
    if config.enable_sample_data:
        adapter = ctx.request_context.lifespan_context.get_adapter()
        return adapter.get_sample_data(table, schema, limit)
    else:
        return []


if __name__ == "__main__":
    app.run()
