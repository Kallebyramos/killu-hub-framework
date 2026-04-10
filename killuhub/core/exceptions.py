class KilluHubError(Exception):
    """Base exception for all KilluHub errors."""


class ConnectorNotFoundError(KilluHubError):
    def __init__(self, name: str):
        super().__init__(f"Connector '{name}' not found in registry.")


class EngineNotFoundError(KilluHubError):
    def __init__(self, name: str):
        super().__init__(f"Engine '{name}' not found in registry.")


class StorageWriterNotFoundError(KilluHubError):
    def __init__(self, name: str):
        super().__init__(f"Storage writer '{name}' not found in registry.")


class ConnectorConfigError(KilluHubError):
    pass


class PipelineError(KilluHubError):
    pass


class BatchModeError(KilluHubError):
    """Raised when an invalid or unsupported BatchMode is used."""
    pass


class IncrementalStateError(KilluHubError):
    """Raised when the incremental state store fails to read or write state."""
    pass


class ContractViolationError(KilluHubError):
    """Raised when a data contract check fails and on_violation='fail'."""
    pass
