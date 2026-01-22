"""Custom exceptions and HTTP error mapping for Quickbase."""

class QuickbaseError(Exception):
    """class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class QuickbaseBackoffError(QuickbaseError):
    """class representing backoff error handling."""


class QuickbaseBadRequestError(QuickbaseError):
    """class representing 400 status code."""


class QuickbaseUnauthorizedError(QuickbaseError):
    """class representing 401 status code."""



class QuickbaseForbiddenError(QuickbaseError):
    """class representing 403 status code."""


class QuickbaseNotFoundError(QuickbaseError):
    """class representing 404 status code."""


class QuickbaseConflictError(QuickbaseError):
    """class representing 409 status code."""


class QuickbaseUnprocessableEntityError(QuickbaseBackoffError):
    """class representing 422 status code."""


class QuickbaseRateLimitError(QuickbaseBackoffError):
    """class representing 429 status code."""


class QuickbaseInternalServerError(QuickbaseBackoffError):
    """class representing 500 status code."""


class QuickbaseNotImplementedError(QuickbaseBackoffError):
    """class representing 501 status code."""


class QuickbaseBadGatewayError(QuickbaseBackoffError):
    """class representing 502 status code."""


class QuickbaseServiceUnavailableError(QuickbaseBackoffError):
    """class representing 503 status code."""


ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": QuickbaseBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": QuickbaseUnauthorizedError,
        "message": (
            "The access token provided is expired, revoked, malformed or invalid "
            "for other reasons."
        ),
    },
    403: {
        "raise_exception": QuickbaseForbiddenError,
        "message": "You are missing the following required scopes: read"
    },
    404: {
        "raise_exception": QuickbaseNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": QuickbaseConflictError,
        "message": (
            "The API request cannot be completed because the requested operation "
            "would conflict with an existing item."
        ),
    },
    422: {
        "raise_exception": QuickbaseUnprocessableEntityError,
        "message": "The request content itself is not processable by the server."
    },
    429: {
        "raise_exception": QuickbaseRateLimitError,
        "message": (
            "The API rate limit for your organisation/application pairing has been "
            "exceeded."
        ),
    },
    500: {
        "raise_exception": QuickbaseInternalServerError,
        "message": (
            "The server encountered an unexpected condition which prevented "
            "it from fulfilling the request."
        ),
    },
    501: {
        "raise_exception": QuickbaseNotImplementedError,
        "message": (
            "The server does not support the functionality required to fulfill "
            "the request."
        ),
    },
    502: {
        "raise_exception": QuickbaseBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": QuickbaseServiceUnavailableError,
        "message": "API service is currently unavailable."
    }
}
