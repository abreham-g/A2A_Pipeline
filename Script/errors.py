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
