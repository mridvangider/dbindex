# Introduction

`dbindex` is an MCP server that provides access to various databases. It provides the following tools:
- `get_all_schemas() -> list[dict]`
- `get_all_tables(schema: str | None = None) -> list[dict]`
- `get_all_columns(schema: str | None = None, table: str | None = None) -> list[dict]`
- `get_sample_data(table: str, schema: str | None = None, limit: int = 10) -> list[dict]`

Each tool returns a `list[dict]`

The connection to the databases are provided by `Adapter` subclasses. Each adapter implements the tools above. In addition, they implement `__enter__`, `__exit__`