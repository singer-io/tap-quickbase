"""
Unit tests for client authentication and headers.
Following DRY, KISS, YAGNI principles.
"""
import unittest
from unittest.mock import patch, MagicMock
from parameterized import parameterized
from tap_quickbase.client import Client


class TestAuthentication(unittest.TestCase):
    """Test authentication header construction."""

    def setUp(self):
        """Common setup."""
        self.config = {
            "access_token": "test_token_12345",
            "realm_hostname": "mycompany.quickbase.com",
            "request_timeout": 30
        }
        self.client = Client(self.config)

    def test_authenticate_adds_user_token(self):
        """Test that QB-USER-TOKEN header is added correctly."""
        headers = {}
        params = {}
        
        headers, params = self.client.authenticate(headers, params)
        
        self.assertIn("Authorization", headers)
        self.assertEqual(headers["Authorization"], "QB-USER-TOKEN test_token_12345")

    def test_authenticate_adds_realm_hostname(self):
        """Test that QB-Realm-Hostname header is added."""
        headers = {}
        params = {}
        
        headers, params = self.client.authenticate(headers, params)
        
        self.assertIn("QB-Realm-Hostname", headers)
        self.assertEqual(headers["QB-Realm-Hostname"], "mycompany.quickbase.com")

    def test_authenticate_uses_default_realm(self):
        """Test default realm hostname when not provided."""
        config = {
            "access_token": "test_token",
            "request_timeout": 30
        }
        client = Client(config)
        headers, params = client.authenticate({}, {})
        
        self.assertEqual(headers["QB-Realm-Hostname"], "api.quickbase.com")

    def test_authenticate_adds_user_agent(self):
        """Test that User-Agent header is added."""
        headers = {}
        params = {}
        
        headers, params = self.client.authenticate(headers, params)
        
        self.assertIn("User-Agent", headers)
        self.assertEqual(headers["User-Agent"], "tap-quickbase/1.0.0")

    def test_authenticate_preserves_existing_headers(self):
        """Test that existing headers are preserved."""
        headers = {"Custom-Header": "custom_value"}
        params = {"param1": "value1"}
        
        headers, params = self.client.authenticate(headers, params)
        
        self.assertEqual(headers["Custom-Header"], "custom_value")
        self.assertEqual(params["param1"], "value1")

    @parameterized.expand([
        ["realm1.quickbase.com"],
        ["company-test.quickbase.com"],
        ["api.quickbase.com"]
    ])
    def test_authenticate_with_various_realms(self, realm):
        """Test authentication with different realm hostnames."""
        config = {
            "access_token": "token",
            "realm_hostname": realm,
            "request_timeout": 30
        }
        client = Client(config)
        headers, params = client.authenticate({}, {})
        
        self.assertEqual(headers["QB-Realm-Hostname"], realm)


class TestClientInitialization(unittest.TestCase):
    """Test client initialization with various configurations."""

    @parameterized.expand([
        ["default_timeout", {}, 300.0],
        ["custom_timeout_int", {"request_timeout": 60}, 60.0],
        ["custom_timeout_float", {"request_timeout": 45.5}, 45.5],
        ["custom_timeout_string", {"request_timeout": "120"}, 120.0]
    ])
    @patch("tap_quickbase.client.session")
    def test_request_timeout_initialization(self, name, extra_config, expected, mock_session):
        """Test request timeout initialization."""
        config = {"access_token": "token", **extra_config}
        client = Client(config)
        
        self.assertEqual(client.request_timeout, expected)

    def test_base_url_initialization(self):
        """Test base URL is set correctly."""
        config = {"access_token": "token"}
        client = Client(config)
        
        self.assertEqual(client.base_url, "https://api.quickbase.com")


class TestMakeRequest(unittest.TestCase):
    """Test make_request method applies authentication."""

    def setUp(self):
        """Common setup."""
        self.config = {
            "access_token": "test_token",
            "realm_hostname": "test.quickbase.com"
        }
        self.client = Client(self.config)

    @patch("tap_quickbase.client.Client._Client__make_request")
    def test_make_request_applies_authentication(self, mock_make_request):
        """Test that make_request applies authentication headers."""
        mock_make_request.return_value = {"data": "test"}
        
        self.client.make_request("GET", "https://api.quickbase.com/v1/apps")
        
        # Verify __make_request was called
        call_args = mock_make_request.call_args
        headers = call_args.kwargs.get("headers", {})
        
        self.assertIn("Authorization", headers)
        self.assertIn("QB-Realm-Hostname", headers)
        self.assertIn("User-Agent", headers)

    @patch("tap_quickbase.client.Client._Client__make_request")
    def test_make_request_with_custom_headers(self, mock_make_request):
        """Test make_request preserves custom headers."""
        mock_make_request.return_value = {"data": "test"}
        custom_headers = {"X-Custom": "value"}
        
        self.client.make_request(
            "GET", 
            "https://api.quickbase.com/v1/apps",
            headers=custom_headers
        )
        
        call_args = mock_make_request.call_args
        headers = call_args.kwargs.get("headers", {})
        
        self.assertEqual(headers["X-Custom"], "value")
        self.assertIn("Authorization", headers)
