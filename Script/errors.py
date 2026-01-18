"""Project exception types."""


class RocketSourceError(Exception):
    """Base exception for RocketSource automation errors."""

    pass


class ConfigError(RocketSourceError):
    """Raised when required configuration is missing or invalid."""

    pass


class ApiRequestError(RocketSourceError):
    """Raised when an HTTP request fails (transport or non-2xx status)."""

    pass


class ApiResponseError(RocketSourceError):
    """Raised when the API response payload is not in the expected format."""

    pass


class ScanFailedError(RocketSourceError):
    """Raised when a scan reports a failure status."""

    pass


class ScanTimeoutError(RocketSourceError):
    """Raised when polling exceeds the configured timeout."""

    pass
# errors.py - Add these new exception classes
class ScanInProgressError(Exception):
    """Raised when a scan is already in progress and another cannot be started."""
    pass

class RateLimitError(Exception):
    """Raised when rate limited or hitting concurrent scan limits."""
    def __init__(self, message: str, retry_after: int = 30):
        super().__init__(message)
        self.retry_after = retry_after

# Keep your existing exceptions:
class ApiRequestError(Exception):
    """Raised when an API request fails."""
    pass

class ApiResponseError(Exception):
    """Raised when an API response is malformed or unexpected."""
    pass

class ScanFailedError(Exception):
    """Raised when a scan completes with a failed status."""
    pass

class ScanTimeoutError(Exception):
    """Raised when polling for scan status times out."""
    pass