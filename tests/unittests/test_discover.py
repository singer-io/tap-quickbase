"""Unit tests for catalog discovery."""
import unittest
from unittest.mock import patch, MagicMock
from tap_quickbase.discover import discover


class TestDiscover(unittest.TestCase):
    """Test catalog discovery."""

    @patch("tap_quickbase.discover.get_schemas")
    def test_discover_creates_catalog(self, mock_get_schemas):
        """Test discover returns a catalog with streams."""
        mock_get_schemas.return_value = (
            {
                "apps": {
                    "type": "object",
                    "properties": {"id": {"type": "string"}}
                }
            },
            {
                "apps": [
                    {"breadcrumb": [], "metadata": {"table-key-properties": ["id"]}}
                ]
            }
        )
        
        catalog = discover()
        
        self.assertIsNotNone(catalog)
        self.assertEqual(len(catalog.streams), 1)
        self.assertEqual(catalog.streams[0].stream, "apps")
        self.assertEqual(catalog.streams[0].tap_stream_id, "apps")

    @patch("tap_quickbase.discover.get_schemas")
    def test_discover_handles_multiple_streams(self, mock_get_schemas):
        """Test discover handles multiple streams."""
        mock_get_schemas.return_value = (
            {
                "apps": {"type": "object", "properties": {"id": {"type": "string"}}},
                "tables": {"type": "object", "properties": {"id": {"type": "string"}}}
            },
            {
                "apps": [{"breadcrumb": [], "metadata": {"table-key-properties": ["id"]}}],
                "tables": [{"breadcrumb": [], "metadata": {"table-key-properties": ["id"]}}]
            }
        )
        
        catalog = discover()
        
        self.assertEqual(len(catalog.streams), 2)
        stream_names = [s.stream for s in catalog.streams]
        self.assertIn("apps", stream_names)
        self.assertIn("tables", stream_names)

    @patch("tap_quickbase.discover.get_schemas")
    def test_discover_raises_error_on_invalid_schema(self, mock_get_schemas):
        """Test discover raises error when schema is invalid."""
        mock_get_schemas.return_value = (
            {"apps": "invalid_schema"},
            {"apps": []}
        )
        
        with self.assertRaises(Exception):
            discover()
