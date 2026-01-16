"""Unit tests for Client class - authentication, requests, errors, and backoff."""
import unittest
from unittest.mock import MagicMock, patch
from parameterized import parameterized
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
from tap_quickbase.client import Client
from tap_quickbase.client import REQUEST_TIMEOUT as DEFAULT_REQUEST_TIMEOUT
from tap_quickbase.exceptions import (
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


class TestClientInitialization(unittest.TestCase):
    """Test client initialization and configuration."""

    @parameterized.expand([
        ["empty_value", {}, DEFAULT_REQUEST_TIMEOUT],
        ["string_value", {"request_timeout": "12"}, 12.0],
        ["integer_value", {"request_timeout": 10}, 10.0],
        ["float_value", {"request_timeout": 20.0}, 20.0],
        ["zero_value", {"request_timeout": 0}, DEFAULT_REQUEST_TIMEOUT]
    ])
    @patch("tap_quickbase.client.session")
    def test_request_timeout_initialization(self, name, extra_config, expected, mock_session):
        """Test request timeout handles various input types."""
        config = {"access_token": "token", **extra_config}
        client = Client(config)
        self.assertEqual(client.request_timeout, expected)

    def test_base_url_initialization(self):
        """Test base URL is set correctly."""
        config = {"access_token": "token"}
        client = Client(config)
        self.assertEqual(client.base_url, "https://api.quickbase.com")


class TestAuthentication(unittest.TestCase):
    """Test authentication headers."""

    def setUp(self):
        """Common setup."""
        self.config = {
            "access_token": "test_token",
            "realm_hostname": "mycompany.quickbase.com"
        }
        self.client = Client(self.config)

    def test_authenticate_adds_required_headers(self):
        """Test all required authentication headers are added."""
        headers, params = self.client.authenticate({}, {})
        
        self.assertEqual(headers["Authorization"], "QB-USER-TOKEN test_token")
        self.assertEqual(headers["QB-Realm-Hostname"], "mycompany.quickbase.com")
        self.assertEqual(headers["User-Agent"], "tap-quickbase/1.0.0")

    def test_authenticate_uses_default_realm(self):
        """Test default realm hostname when not provided."""
        config = {"access_token": "token"}
        client = Client(config)
        headers, _ = client.authenticate({}, {})
        
        self.assertEqual(headers["QB-Realm-Hostname"], "api.quickbase.com")

    def test_authenticate_preserves_existing_values(self):
        """Test existing headers and params are preserved."""
        headers = {"Custom-Header": "custom_value"}
        params = {"param1": "value1"}
        
        headers, params = self.client.authenticate(headers, params)
        
        self.assertEqual(headers["Custom-Header"], "custom_value")
        self.assertEqual(params["param1"], "value1")


class TestRequestHandling(unittest.TestCase):
    """Test HTTP request handling."""

    def setUp(self):
        """Common setup."""
        self.config = {"access_token": "token"}
        self.client = Client(self.config)

    def test_get_request_removes_json_body(self):
        """Test GET requests don't send JSON body."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        
        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            self.client._Client__make_request("GET", "https://api.quickbase.com/test", json={"should": "be_removed"})
            
            call_kwargs = mock_request.call_args.kwargs
            self.assertNotIn("json", call_kwargs)
            self.assertNotIn("data", call_kwargs)

    def test_post_request_converts_string_to_json(self):
        """Test POST requests convert string data to JSON."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"success": True}
        
        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            self.client._Client__make_request("POST", "https://api.quickbase.com/test", data='{"key": "value"}')
            
            call_kwargs = mock_request.call_args.kwargs
            self.assertIn("json", call_kwargs)
            self.assertNotIn("data", call_kwargs)


class TestErrorHandling(unittest.TestCase):
    """Test error handling for various HTTP status codes."""

    def setUp(self):
        """Common setup."""
        self.config = {"access_token": "token"}
        self.client = Client(self.config)

    @parameterized.expand([
        ["400", 400, QuickbaseBadRequestError, "A validation exception has occurred."],
        ["401", 401, QuickbaseUnauthorizedError, "The access token provided is expired, revoked, malformed or invalid for other reasons."],
        ["403", 403, QuickbaseForbiddenError, "You are missing the following required scopes: read"],
        ["404", 404, QuickbaseNotFoundError, "The resource you have specified cannot be found."],
        ["409", 409, QuickbaseConflictError, "The API request cannot be completed because the requested operation would conflict with an existing item."],
    ])
    def test_non_retriable_errors(self, name, status_code, exception_class, error_message):
        """Test errors that should not trigger retry."""
        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_response.json.return_value = {}
        
        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            with self.assertRaises(exception_class) as context:
                self.client._Client__make_request("GET", "https://api.quickbase.com/test")
            
            # Should only try once (no retry)
            self.assertEqual(mock_request.call_count, 1)
            self.assertIn(error_message, str(context.exception))


class TestBackoffAndRetry(unittest.TestCase):
    """Test backoff and retry mechanism."""

    def setUp(self):
        """Common setup."""
        self.config = {"access_token": "token"}
        self.client = Client(self.config)

    @parameterized.expand([
        ["422", 422, QuickbaseUnprocessableEntityError],
        ["429", 429, QuickbaseRateLimitError],
        ["500", 500, QuickbaseInternalServerError],
        ["501", 501, QuickbaseNotImplementedError],
        ["502", 502, QuickbaseBadGatewayError],
        ["503", 503, QuickbaseServiceUnavailableError],
    ])
    @patch("time.sleep")
    def test_retriable_http_errors_retry(self, name, status_code, exception_class, mock_sleep):
        """Test retriable HTTP errors trigger retry with backoff."""
        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_response.json.return_value = {}
        
        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            with self.assertRaises(exception_class):
                self.client._Client__make_request("GET", "https://api.quickbase.com/test")
            
            # Should retry 5 times
            self.assertEqual(mock_request.call_count, 5)
            # Should call sleep for backoff between retries
            self.assertTrue(mock_sleep.call_count >= 4)

    @parameterized.expand([
        ["connection_reset", ConnectionResetError],
        ["connection_error", ConnectionError],
        ["chunked_encoding", ChunkedEncodingError],
        ["timeout", Timeout],
    ])
    @patch("time.sleep")
    def test_network_errors_retry(self, name, error_class, mock_sleep):
        """Test network errors trigger retry."""
        with patch.object(self.client._session, "request", side_effect=error_class) as mock_request:
            with self.assertRaises(error_class):
                self.client._Client__make_request("GET", "https://api.quickbase.com/test")
            
            # Should retry 5 times
            self.assertEqual(mock_request.call_count, 5)

    @patch("time.sleep")
    def test_success_after_retry(self, mock_sleep):
        """Test successful request after retries."""
        mock_response_error = MagicMock()
        mock_response_error.status_code = 429
        mock_response_error.json.return_value = {}
        
        mock_response_success = MagicMock()
        mock_response_success.status_code = 200
        mock_response_success.json.return_value = {"data": "success"}
        
        # Fail twice, then succeed
        with patch.object(
            self.client._session,
            "request",
            side_effect=[mock_response_error, mock_response_error, mock_response_success]
        ) as mock_request:
            result = self.client._Client__make_request("GET", "https://api.quickbase.com/test")
            
            self.assertEqual(result, {"data": "success"})
            self.assertEqual(mock_request.call_count, 3)
