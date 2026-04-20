from types import TracebackType
from pydantic import BaseModel
from .exasol_adapter import ExasolAdapter
from dbindex.types import Primitive

class AppConfig(BaseModel):
    """Application configuration model."""

    app_name: str = "dbindex"
    debug: bool = False
    connection_parameters: dict[str, Primitive] | None = None
    enable_sample_data: bool = False


class AppContext:
    """Application context class to hold shared state."""
    
    def __init__(self, config: AppConfig):
        self.config: AppConfig = config
        self._initialized: bool = False
        self.exasol_adapter: ExasolAdapter | None = None

    def __enter__(self):
        self.initialize()
        return self
    
    def __exit__(self, exc_type: type[BaseException], exc: BaseException, tb: TracebackType):
        if self.exasol_adapter:
            self.exasol_adapter.close()
    
    def initialize(self) -> None:
        """Initialize the application context."""
        if not self._initialized:
            if not self.config.connection_parameters:
                raise Exception("Connection parameters are not initialized")

            self._initialized = True
            self.exasol_adapter = ExasolAdapter(connection_params=self.config.connection_parameters)
            self.exasol_adapter.connect()

    def get_adapter(self) -> ExasolAdapter:
        if self.exasol_adapter:
            return self.exasol_adapter
        else:
            raise Exception("ExasolAdapar is null")

    
    @property
    def is_initialized(self) -> bool:
        return self._initialized
