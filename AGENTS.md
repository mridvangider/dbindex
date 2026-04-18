# dbindex: MCP Server for Database Metadata

## Project Structure
```
src/dbadapter.py  # Abstract DBAdapter base class (all adapters must inherit from this)
src/main.py       # CLI entrypoint
pyproject.toml    # Dependencies and dev tools
```

## Python Setup
- Requires Python **3.14+** (unusual version, ensure correct interpreter)
- Use `uv` for package management (lockfile: `uv.lock`)
- Do not run `pytest` automatically

## SQL Instructions
- Use all the columns from the source tables. Do not select specific columns

## Commands
```bash
# Install dependencies
uv sync

# Run tests (uses pytest-dotenv for env loading)
uv run pytest

# Lint & typecheck
uv run ruff check .
```

## Database Adapter Pattern
All database adapters must implement the abstract methods in `DBAdapter`:
- `get_all_schemas() -> List[Dict]`
- `get_all_tables(schema: Optional[str]) -> List[Dict]`
- `get_all_columns(schema, table) -> List[Dict]`
- `get_sample_data(schema, table, limit=10) -> List[Dict]`
- Context manager methods (`__enter__`, `__exit__`)

## Dependencies
- Runtime: `pyexasol>=2.2.0` (Exasol DB client)
- Dev: `pytest>=9.0.3`, `pytest-dotenv>=0.5.2`, `ruff>=0.15.11`
