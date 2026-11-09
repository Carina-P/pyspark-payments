class DataQualityError(Exception):
    """Raised when reference data fails quality checks."""
    pass

class PipelineWriteError(Exception):
    """Raised when writing pipeline output fails."""
    pass
