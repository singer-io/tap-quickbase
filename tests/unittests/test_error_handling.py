"""
Unit tests for error handling and exception mapping.
Following DRY, KISS, YAGNI principles.
"""
import unittest
from unittest.mock import MagicMock, patch
from parameterized import parameterized
from tap_quickbase.client import Client, raise_for_error
from tap_quickbase.exceptions import (
    QuickbaseError,
    QuickbaseBadRequestError,
    QuickbaseUnauthorizedError,
    QuickbaseForbiddenError,
    QuickbaseNotFoundError,
    QuickbaseConflictError,
    QuickbaseUnprocessableEntityError,
    QuickbaseRateLimitError,
    QuickbaseInternalServerError,
    QuickbaseNotImplementedError,
    QuickbaseBadGatewayError,
    QuickbaseServiceUnavailableError
)


class TestRaiseForError(unittest.TestCase):
    """Test raise_for_error function."""

    @parameterized.expand([
        ["bad_request", 400, QuickbaseBadRequestError],
        ["unauthorized", 401, QuickbaseUnauthorizedError],
        ["forbidden", 403, QuickbaseForbiddenError],
        ["not_found", 404, QuickbaseNotFoundError],
        ["conflict", 409, QuickbaseConflictError],
        ["unprocessable", 422, QuickbaseUnprocessableEntityError],
        ["rate_limit", 429, QuickbaseRateLimitError],
        ["internal_server", 500, QuickbaseInternalServerError],
        ["not_implemented", 501, QuickbaseNotImplementedError],
        ["bad_gateway", 502, QuickbaseBadGatewayError],
        ["service_unavailable", 503, QuickbaseServiceUnavailableError]
    ])
    def test_raise_for_error_status_codes(self, name, status_code, exception_class):
        """Test that correct exceptions are raised for status codes."""
        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_response.json.return_value = {}
        
        with self.assertRaises(exception_class):
            raise_for_error(mock_response)

    def test_raise_for_error_with_error_message(self):
        """Test error message extraction from response."""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"error": "Custom error message"}
        
        with self.assertRaises(QuickbaseBadRequestError) as context:
            raise_for_error(mock_response)
        
        self.assertIn("Custom error message", str(context.exception))

    def test_raise_for_error_with_message_field(self):
        """Test error message extraction from 'message' field."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.json.return_value = {"message": "Resource not found"}
        
        with self.assertRaises(QuickbaseNotFoundError) as context:
            raise_for_error(mock_response)
        
        self.assertIn("Resource not found", str(context.exception))

    def test_raise_for_error_no_json(self):
        """Test error handling when response has no JSON."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.side_effect = Exception("No JSON")
        
        with self.assertRaises(QuickbaseInternalServerError):
            raise_for_error(mock_response)

    def test_raise_for_error_unknown_status_code(self):
        """Test handling of unknown status codes."""
        mock_response = MagicMock()
        mock_response.status_code = 418  # I'm a teapot
        mock_response.json.return_value = {}
        
        with self.assertRaises(QuickbaseError):
            raise_for_error(mock_response)

    def test_success_status_codes_dont_raise(self):
        """Test that success status codes don't raise exceptions."""
        for status_code in [200, 201, 204]:
            mock_response = MagicMock()
            mock_response.status_code = status_code
            mock_response.json.return_value = {}
            
            # Should not raise
            try:
                raise_for_error(mock_response)
            except Exception as e:
                self.fail(f"Unexpected exception for {status_code}: {e}")


class TestExceptionAttributes(unittest.TestCase):
    """Test exception attributes."""

    def test_exception_has_message(self):
        """Test that exceptions store message."""
        exc = QuickbaseError("Test message")
        self.assertEqual(exc.message, "Test message")

    def test_exception_has_response(self):
        """Test that exceptions store response."""
        mock_response = MagicMock()
        exc = QuickbaseError("Test", mock_response)
        self.assertEqual(exc.response, mock_response)

    def test_exception_inheritance(self):
        """Test exception class hierarchy."""
        # Backoff errors inherit from QuickbaseBackoffError
        from tap_quickbase.exceptions import QuickbaseBackoffError
        
        self.assertTrue(issubclass(QuickbaseRateLimitError, QuickbaseBackoffError))
        self.assertTrue(issubclass(QuickbaseInternalServerError, QuickbaseBackoffError))
        self.assertTrue(issubclass(QuickbaseServiceUnavailableError, QuickbaseBackoffError))
        
        # Non-backoff errors inherit from QuickbaseError
        self.assertTrue(issubclass(QuickbaseBadRequestError, QuickbaseError))
        self.assertTrue(issubclass(QuickbaseNotFoundError, QuickbaseError))


class TestClientErrorHandling(unittest.TestCase):
    """Test client error handling integration."""

    def setUp(self):
        """Common setup."""
        self.config = {
            "access_token": "test_token",
            "realm_hostname": "test.quickbase.com"
        }
        self.client = Client(self.config)

    def test_client_raises_on_error_response(self):
        """Test that client raises exception on error response."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.json.return_value = {}
        
        with patch.object(self.client._session, "request", return_value=mock_response):
            with self.assertRaises(QuickbaseUnauthorizedError):
                self.client._Client__make_request("GET", "https://api.quickbase.com/test")

    def test_client_returns_json_on_success(self):
        """Test that client returns JSON on successful response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "success"}
        
        with patch.object(self.client._session, "request", return_value=mock_response):
            result = self.client._Client__make_request("GET", "https://api.quickbase.com/test")
        
        self.assertEqual(result, {"data": "success"})


class TestErrorMessages(unittest.TestCase):
    """Test error message formatting."""

    def test_error_message_format_with_error_field(self):
        """Test error message formatting with 'error' field."""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"error": "Validation failed"}
        
        with self.assertRaises(QuickbaseBadRequestError) as context:
            raise_for_error(mock_response)
        
        error_msg = str(context.exception)
        self.assertIn("HTTP-error-code: 400", error_msg)
        self.assertIn("Validation failed", error_msg)

    def test_error_message_format_with_message_field(self):
        """Test error message formatting with 'message' field."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.json.return_value = {"message": "App not found"}
        
        with self.assertRaises(QuickbaseNotFoundError) as context:
            raise_for_error(mock_response)
        
        error_msg = str(context.exception)
        self.assertIn("HTTP-error-code: 404", error_msg)
        self.assertIn("App not found", error_msg)

    def test_error_message_uses_default_when_no_message(self):
        """Test that default error message is used when response has no message."""
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.json.return_value = {}
        
        with self.assertRaises(QuickbaseForbiddenError) as context:
            raise_for_error(mock_response)
        
        error_msg = str(context.exception)
        self.assertIn("HTTP-error-code: 403", error_msg)
        # Should contain default message from ERROR_CODE_EXCEPTION_MAPPING
        self.assertTrue(len(error_msg) > 0)


class TestErrorRecovery(unittest.TestCase):
    """Test error recovery scenarios."""

    def setUp(self):
        """Common setup."""
        self.config = {"access_token": "test", "realm_hostname": "test.quickbase.com"}
        self.client = Client(self.config)

    @patch("time.sleep")
    def test_recoverable_error_retries(self, mock_sleep):
        """Test that recoverable errors trigger retries."""
        mock_response_error = MagicMock()
        mock_response_error.status_code = 503
        mock_response_error.json.return_value = {}
        
        with patch.object(self.client._session, "request", return_value=mock_response_error) as mock_request:
            with self.assertRaises(QuickbaseServiceUnavailableError):
                self.client._Client__make_request("GET", "https://api.quickbase.com/test")
            
            # Should retry
            self.assertGreater(mock_request.call_count, 1)

    def test_non_recoverable_error_no_retry(self):
        """Test that non-recoverable errors don't retry."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.json.return_value = {}
        
        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            with self.assertRaises(QuickbaseNotFoundError):
                self.client._Client__make_request("GET", "https://api.quickbase.com/test")
            
            # Should only try once
            self.assertEqual(mock_request.call_count, 1)
