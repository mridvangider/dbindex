from pydantic import BaseModel, ConfigDict
from exasol_adapter import ExasolAdapter

class AppConfig(BaseModel):
    """Application configuration model."""
    
    model_config = ConfigDict(extra="allow")
    
    app_name: str = "dbindex"
    debug: bool = False
    connection_parameters: dict
    enable_sample_data: bool = False


class AppContext:
    """Application context class to hold shared state."""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self._initialized = False
        self.exasol_adapter = None

    def __enter__(self):
        self.initialize()
        return self
    
    def __exit__(self, exc_type, exc, tb):
        if self.exasol_adapter:
            self.exasol_adapter.close()
    
    def initialize(self) -> None:
        """Initialize the application context."""
        if not self._initialized:
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
