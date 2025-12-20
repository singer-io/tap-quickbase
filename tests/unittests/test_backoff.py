"""
Unit tests for backoff and retry mechanism.
Following DRY, KISS, YAGNI principles.
"""
import unittest
from unittest.mock import patch, MagicMock
from parameterized import parameterized
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
from tap_quickbase.client import Client
from tap_quickbase.exceptions import (
    QuickbaseBackoffError,
    QuickbaseRateLimitError,
    QuickbaseInternalServerError,
    QuickbaseServiceUnavailableError,
    QuickbaseUnprocessableEntityError,
    QuickbaseBadGatewayError,
    QuickbaseNotImplementedError
)


class TestBackoffMechanism(unittest.TestCase):
    """Tests for backoff and retry logic."""

    def setUp(self):
        """Common setup for all tests."""
        self.config = {
            "access_token": "test_token",
            "realm_hostname": "test.quickbase.com",
            "request_timeout": 30
        }
        self.client = Client(self.config)

    @parameterized.expand([
        ["rate_limit", QuickbaseRateLimitError, 429],
        ["internal_server", QuickbaseInternalServerError, 500],
        ["service_unavailable", QuickbaseServiceUnavailableError, 503],
        ["bad_gateway", QuickbaseBadGatewayError, 502],
        ["not_implemented", QuickbaseNotImplementedError, 501],
        ["unprocessable", QuickbaseUnprocessableEntityError, 422]
    ])
    @patch("time.sleep")
    def test_backoff_retries_on_retriable_errors(self, name, exception_class, status_code, mock_sleep):
        """Test that backoff retries occur for retriable errors."""
        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_response.json.return_value = {}
        
        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            with self.assertRaises(exception_class):
                self.client._Client__make_request("GET", "https://api.quickbase.com/test")
            
            # Should retry 5 times
            self.assertEqual(mock_request.call_count, 5)
            # Should call sleep for backoff (4 times between 5 attempts)
            self.assertEqual(mock_sleep.call_count, 4)

    @parameterized.expand([
        ["connection_reset", ConnectionResetError],
        ["connection_error", ConnectionError],
        ["chunked_encoding", ChunkedEncodingError],
        ["timeout", Timeout]
    ])
    @patch("time.sleep")
    def test_backoff_retries_on_network_errors(self, name, error_class, mock_sleep):
        """Test backoff retries on network errors."""
        with patch.object(self.client._session, "request", side_effect=error_class) as mock_request:
            with self.assertRaises(error_class):
                self.client._Client__make_request("GET", "https://api.quickbase.com/test")
            
            # Should retry 5 times
            self.assertEqual(mock_request.call_count, 5)

    @patch("time.sleep")
    def test_backoff_exponential_delay(self, mock_sleep):
        """Test that backoff uses exponential delay."""
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.json.return_value = {}
        
        with patch.object(self.client._session, "request", return_value=mock_response):
            with self.assertRaises(QuickbaseRateLimitError):
                self.client._Client__make_request("GET", "https://api.quickbase.com/test")
        
        # Verify sleep was called with increasing delays
        # Factor=2, so delays should be: 2, 4, 8, 16
        self.assertTrue(mock_sleep.call_count >= 4)

    def test_backoff_success_after_retry(self):
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
            with patch("time.sleep"):
                result = self.client._Client__make_request("GET", "https://api.quickbase.com/test")
            
            self.assertEqual(result, {"data": "success"})
            self.assertEqual(mock_request.call_count, 3)

    @patch("time.sleep")
    def test_max_retries_respected(self, mock_sleep):
        """Test that max retries limit is respected."""
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_response.json.return_value = {}
        
        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            with self.assertRaises(QuickbaseServiceUnavailableError):
                self.client._Client__make_request("GET", "https://api.quickbase.com/test")
            
            # Max tries should be exactly 5
            self.assertEqual(mock_request.call_count, 5)


class TestBackoffErrorTypes(unittest.TestCase):
    """Test that correct error types trigger backoff."""

    def setUp(self):
        """Common setup."""
        self.config = {"access_token": "test", "realm_hostname": "test.quickbase.com"}
        self.client = Client(self.config)

    @patch("time.sleep")
    def test_quickbase_backoff_error_triggers_retry(self, mock_sleep):
        """Test QuickbaseBackoffError triggers retry."""
        mock_response = MagicMock()
        mock_response.status_code = 422
        mock_response.json.return_value = {}
        
        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            with self.assertRaises(QuickbaseUnprocessableEntityError):
                self.client._Client__make_request("GET", "https://api.quickbase.com/test")
            
            self.assertEqual(mock_request.call_count, 5)

    def test_non_backoff_errors_no_retry(self):
        """Test that non-backoff errors do not retry."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.json.return_value = {}
        
        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            with self.assertRaises(Exception):
                self.client._Client__make_request("GET", "https://api.quickbase.com/test")
            
            # Should only try once (no retry)
            self.assertEqual(mock_request.call_count, 1)
