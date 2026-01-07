import unittest
from unittest.mock import patch, MagicMock
from tap_quickbase.streams.abstracts import IncrementalStream

class ConcreteParentBaseStream(IncrementalStream):
    @property
    def key_properties(self):
        return ["id"]

    @property
    def replication_keys(self):
        return ["updated_at"]

    @property
    def replication_method(self):
        return "FULL_TABLE"

    @property
    def tap_stream_id(self):
        return "stream_1"

class TestSync(unittest.TestCase):
    @patch("tap_quickbase.streams.abstracts.metadata.to_map")
    def setUp(self, mock_to_map):

        mock_catalog = MagicMock()
        mock_catalog.schema.to_dict.return_value = {"key": "value"}
        mock_catalog.metadata = "mock_metadata"
        mock_to_map.return_value = {"metadata_key": "metadata_value"}

        self.stream = ConcreteParentBaseStream(catalog=mock_catalog)
        self.stream.client = MagicMock()
        self.stream.child_to_sync = []

    @patch("tap_quickbase.streams.abstracts.get_bookmark", return_value=100)
    def test_write_bookmark_with_state(self, mock_get_bookmark):

        state = {'bookmarks': {'stream_1': {'updated_at': 100}}}
        result = self.stream.write_bookmark(state, "stream_1", "updated_at", 200)
        self.assertEqual(result, {'bookmarks': {'stream_1': {'updated_at': 200}}})

    @patch("tap_quickbase.streams.abstracts.get_bookmark", return_value=100)
    def test_write_bookmark_without_state(self, mock_get_bookmark):

        state = {}
        result = self.stream.write_bookmark(state, "stream_1", "updated_at", 200)
        self.assertEqual(result, {'bookmarks': {'stream_1': {'updated_at': 200}}})

    @patch("tap_quickbase.streams.abstracts.get_bookmark", return_value=300)
    def test_write_bookmark_with_old_value(self, mock_get_bookmark):

        state = {'bookmarks': {'stream_1': {'updated_at': 300}}}
        result = self.stream.write_bookmark(state, "stream_1", "updated_at", 200)
        self.assertEqual(result, {'bookmarks': {'stream_1': {'updated_at': 300}}})
