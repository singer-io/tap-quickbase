import unittest
import requests
from unittest.mock import patch
from parameterized import parameterized
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
from tap_quickbase.client import Client
from tap_quickbase.exceptions import *


default_config = {
    "base_url": "https://api.example.com",
    "request_timeout": 30,
    "access_token": "dummy_token",
}

DEFAULT_REQUEST_TIMEOUT = 300

class MockResponse:
    """Mocked standard HTTPResponse to test error handling."""

    def __init__(
        self, status_code, resp = "", content=[""], headers=None, raise_error=True, text={}
    ):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error
        self.text = text
        self.reason = "error"

    def raise_for_status(self):
        """If an error occur, this method returns a HTTPError object.

        Raises:
            requests.HTTPError: Mock http error.

        Returns:
            int: Returns status code if not error occurred.
        """
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("mock sample message")

    def json(self):
        """Returns a JSON object of the result."""
        return self.text

class TestClient(unittest.TestCase):

    def setUp(self):
        """Set up the client with default configuration."""
        self.client = Client(default_config)

    @parameterized.expand([    
        ["empty value", "", DEFAULT_REQUEST_TIMEOUT],
        ["string value", "12", 12.0],
        ["integer value", 10, 10.0],
        ["float value", 20.0, 20.0],
        ["zero value", 0, DEFAULT_REQUEST_TIMEOUT]
    ])
    @patch("tap_quickbase.client.session")
    def test_client_initialization(self, test_name, input_value, expected_value, mock_session):
        default_config["request_timeout"] = input_value
        client = Client(default_config)
        assert client.request_timeout == expected_value
        assert isinstance(client._session, mock_session().__class__)


    @patch("tap_quickbase.client.Client._Client__make_request")
    def test_client_make_request_get(self, mock_make_request):
        """Test make_request with GET method."""
        mock_make_request.return_value = {"data": "ok"}
        result = self.client.make_request("GET", "https://api.example.com/resource")
        assert result == {"data": "ok"}
        mock_make_request.assert_called_once()

    @patch("tap_quickbase.client.Client._Client__make_request")
    def test_client_make_request_post(self, mock_make_request):
        """Test make_request with POST method."""
        mock_make_request.return_value = {"created": True}
        result = self.client.make_request("POST", "https://api.example.com/resource", body={"key": "value"})
        assert result == {"created": True}
        mock_make_request.assert_called_once()

    def test_get_request_removes_json_body(self):
        """Test that GET requests don't send JSON body."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        
        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            self.client._Client__make_request("GET", "https://api.example.com/resource", json={"should": "be_removed"})
            
            # Verify 'json' is not in kwargs
            call_kwargs = mock_request.call_args.kwargs
            self.assertNotIn("json", call_kwargs)
            self.assertNotIn("data", call_kwargs)

    def test_post_request_converts_string_to_json(self):
        """Test that POST requests convert string data to JSON."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"success": True}
        
        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            self.client._Client__make_request("POST", "https://api.example.com/resource", data='{"key": "value"}')
            
            call_kwargs = mock_request.call_args.kwargs
            self.assertIn("json", call_kwargs)
            self.assertNotIn("data", call_kwargs)

    @parameterized.expand([
        ["400 error", 400, MockResponse(400), QuickbaseBadRequestError, "A validation exception has occurred."],
        ["401 error", 401, MockResponse(401), QuickbaseUnauthorizedError, "The access token provided is expired, revoked, malformed or invalid for other reasons."],
        ["403 error", 403, MockResponse(403), QuickbaseForbiddenError, "You are missing the following required scopes: read"],
        ["404 error", 404, MockResponse(404), QuickbaseNotFoundError, "The resource you have specified cannot be found."],
        ["409 error", 409, MockResponse(409), QuickbaseConflictError, "The API request cannot be completed because the requested operation would conflict with an existing item."],
    ])
    def test_make_request_http_failure_without_retry(self, test_name, error_code, mock_response, error, error_message):
        
        with patch.object(self.client._session, "request", return_value=mock_response):
            with self.assertRaises(error) as e:
                self.client._Client__make_request("GET", "https://api.example.com/resource")

        expected_error_message = (f"HTTP-error-code: {error_code}, Error: {error_message}")
        self.assertEqual(str(e.exception), expected_error_message)

    @parameterized.expand([
        ["422 error", 422, MockResponse(422), QuickbaseUnprocessableEntityError, "The request content itself is not processable by the server."],
        ["429 error", 429, MockResponse(429), QuickbaseRateLimitError, "The API rate limit for your organisation/application pairing has been exceeded."],
        ["500 error", 500, MockResponse(500), QuickbaseInternalServerError, "The server encountered an unexpected condition which prevented it from fulfilling the request."],
        ["501 error", 501, MockResponse(501), QuickbaseNotImplementedError, "The server does not support the functionality required to fulfill the request."],
        ["502 error", 502, MockResponse(502), QuickbaseBadGatewayError, "Server received an invalid response."],
        ["503 error", 503, MockResponse(503), QuickbaseServiceUnavailableError, "API service is currently unavailable."],
    ])
    @patch("time.sleep")
    def test_make_request_http_failure_with_retry(self, test_name, error_code, mock_response, error, error_message, mock_sleep):
        
        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            with self.assertRaises(error) as e:
                self.client._Client__make_request("GET", "https://api.example.com/resource")

            expected_error_message = (f"HTTP-error-code: {error_code}, Error: {error_message}")
            self.assertEqual(str(e.exception), expected_error_message)
            self.assertEqual(mock_request.call_count, 5)

    @parameterized.expand([
        ["ConnectionResetError", ConnectionResetError],
        ["ConnectionError", ConnectionError],
        ["ChunkedEncodingError", ChunkedEncodingError],
        ["Timeout", Timeout],
    ])
    @patch("time.sleep")
    def test_make_request_other_failure_with_retry(self, test_name, error, mock_sleep):
        
        with patch.object(self.client._session, "request", side_effect=error) as mock_request:
            with self.assertRaises(error) as e:
                self.client._Client__make_request("GET", "https://api.example.com/resource")
            
            self.assertEqual(mock_request.call_count, 5)
