from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
import json
import sys

from mcp.server.fastmcp import FastMCP

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


if __name__ == "__main__":
    app.run()
