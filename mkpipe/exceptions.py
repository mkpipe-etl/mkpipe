class MkpipeError(Exception):
    """Base exception for all mkpipe errors."""


class ConfigError(MkpipeError):
    """Configuration file or value errors."""


class ExtractionError(MkpipeError):
    """Failures during data extraction."""


class LoadError(MkpipeError):
    """Failures during data loading."""


class TransformError(MkpipeError):
    """Failures during data transformation."""


class PluginNotFoundError(MkpipeError):
    """Requested plugin variant not installed or registered."""


class BackendError(MkpipeError):
    """Backend manifest operation failures."""
