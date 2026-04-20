from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

import json
from pathlib import Path
import sys

from mcp.server.fastmcp import Context, FastMCP
from mcp.server.session import ServerSession

from dbindex.app_context import AppContext, AppConfig
from dbindex.types import Primitive, Row

config: AppConfig | None = None

def load_config(path: Path) -> AppConfig:
    with open(path, "r") as f:
        _config: dict[str, Primitive] = json.load(f)
        config = AppConfig.model_validate(_config)
        return config

@asynccontextmanager
async def app_lifespan(_: FastMCP) -> AsyncIterator[AppContext]:
    if not config:
        raise Exception("Config is not loaded")

    with AppContext(config) as context:
        yield context

app = FastMCP("dbindex", lifespan=app_lifespan, json_response=True)


# Access type-safe lifespan context in tools
@app.tool()
def get_all_schemas(ctx: Context[ServerSession, AppContext]) -> list[Row]:
    """Get all available schemas from the database.
    """
    adapter = ctx.request_context.lifespan_context.get_adapter()
    return adapter.get_all_schemas()


@app.tool()
def get_all_tables(
    ctx: Context[ServerSession, AppContext], schema: str | None = None
) -> list[Row]:
    """Get all tables in the database (or specific schema).

    Args:
        schema: Optional schema name to filter tables. If None, returns tables from all schemas.
    """
    adapter = ctx.request_context.lifespan_context.get_adapter()
    return adapter.get_all_tables(schema)


@app.tool()
def get_all_columns(
    ctx: Context[ServerSession, AppContext], schema: str | None = None, table: str | None = None
) -> list[Row]:
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
    schema: str | None = None,
    limit: int = 10
) -> list[Row]:
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
    import argparse

    parser = argparse.ArgumentParser(prog="dbindex", description="An MCP server that provides access to database metadata")
    _ = parser.add_argument('-c', '--config', type=Path, required=True)
    _ = parser.add_argument('-t', '--transport', choices=['stdio', 'streamable-http'], required=True)
    args = parser.parse_args(sys.argv)

    config = load_config(args.config)
    transport = args.transport

    app.run(transport=transport)
