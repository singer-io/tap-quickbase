import unittest
from unittest.mock import patch, MagicMock, call
from datetime import timedelta
from tap_quickbase.streams.abstracts import IncrementalStream, PseudoIncrementalStream
import singer.utils as singer_utils


class ConcreteIncrementalStream(IncrementalStream):
    @property
    def key_properties(self):
        return ["id"]

    @property
    def replication_keys(self):
        return ["updated_at"]

    @property
    def replication_method(self):
        return "INCREMENTAL"

    @property
    def tap_stream_id(self):
        return "stream_1"


class ConcretePseudoIncrementalStream(PseudoIncrementalStream):
    @property
    def key_properties(self):
        return ["id"]

    @property
    def replication_method(self):
        return "FULL_TABLE"

    @property
    def tap_stream_id(self):
        return "pseudo_stream"


class TestIncrementalSync(unittest.TestCase):
    """Test suite for IncrementalStream bookmark management."""

    @patch("tap_quickbase.streams.abstracts.metadata.to_map")
    def setUp(self, mock_to_map):
        mock_catalog = MagicMock()
        mock_catalog.schema.to_dict.return_value = {"key": "value"}
        mock_catalog.metadata = "mock_metadata"
        mock_to_map.return_value = {"metadata_key": "metadata_value"}

        self.stream = ConcreteIncrementalStream(catalog=mock_catalog)
        self.stream.client = MagicMock()
        self.stream.child_to_sync = []

    @patch("tap_quickbase.streams.abstracts.get_bookmark", return_value=100)
    def test_write_bookmark_updates_with_newer_value(self, mock_get_bookmark):
        """Test bookmark is updated when new value is greater."""
        state = {'bookmarks': {'stream_1': {'updated_at': 100}}}
        result = self.stream.write_bookmark(state, "stream_1", "updated_at", 200)
        self.assertEqual(result, {'bookmarks': {'stream_1': {'updated_at': 200}}})

    @patch("tap_quickbase.streams.abstracts.get_bookmark", return_value=300)
    def test_write_bookmark_preserves_older_value(self, mock_get_bookmark):
        """Test bookmark is NOT updated when new value is older."""
        state = {'bookmarks': {'stream_1': {'updated_at': 300}}}
        result = self.stream.write_bookmark(state, "stream_1", "updated_at", 200)
        self.assertEqual(result, {'bookmarks': {'stream_1': {'updated_at': 300}}})


class TestPseudoIncrementalSync(unittest.TestCase):
    """Test suite for PseudoIncrementalStream bookmark and filtering logic."""

    @patch("tap_quickbase.streams.abstracts.metadata.to_map")
    def setUp(self, mock_to_map):
        """Set up test fixtures."""
        mock_catalog = MagicMock()
        mock_catalog.schema.to_dict.return_value = {
            "properties": {
                "id": {"type": ["string"]},
                "updated": {"type": ["string"], "format": "date-time"}
            }
        }
        mock_catalog.metadata = "mock_metadata"
        mock_to_map.return_value = {"metadata_key": "metadata_value"}

        self.stream = ConcretePseudoIncrementalStream(catalog=mock_catalog)
        self.stream.client = MagicMock()
        self.stream.client.config = {"start_date": "2023-01-01T00:00:00Z"}
        self.stream.child_to_sync = []

    @patch("tap_quickbase.streams.abstracts.get_bookmark")
    def test_get_bookmark_subtracts_one_second(self, mock_get_bookmark):
        """Test bookmark has 1 second subtracted for overlap window."""
        mock_get_bookmark.return_value = "2025-12-18T11:28:44Z"
        state = {"bookmarks": {"pseudo_stream": {"updated": "2025-12-18T11:28:44Z"}}}
        
        result = self.stream._get_bookmark(state)
        
        # Should return bookmark minus 1 second
        expected = singer_utils.strptime_to_utc("2025-12-18T11:28:44Z") - timedelta(seconds=1)
        self.assertEqual(result, expected)

    @patch("tap_quickbase.streams.abstracts.get_bookmark")
    def test_get_bookmark_uses_start_date_when_no_bookmark(self, mock_get_bookmark):
        """Test start_date is used when no bookmark exists (first sync)."""
        mock_get_bookmark.return_value = None
        state = {}
        
        result = self.stream._get_bookmark(state)
        
        # Should return start_date minus 1 second
        expected = singer_utils.strptime_to_utc("2023-01-01T00:00:00Z") - timedelta(seconds=1)
        self.assertEqual(result, expected)

    @patch("tap_quickbase.streams.abstracts.write_state")
    @patch("tap_quickbase.streams.abstracts.write_bookmark")
    @patch("tap_quickbase.streams.abstracts.get_bookmark")
    def test_write_bookmark_updates_with_newer_value(self, mock_get_bookmark, mock_write_bookmark, mock_write_state):
        """Test bookmark is written when new value is newer."""
        mock_get_bookmark.return_value = "2025-12-18T11:28:40Z"
        mock_write_bookmark.return_value = {"bookmarks": {"pseudo_stream": {"updated": "2025-12-18T11:28:44Z"}}}
        
        state = {"bookmarks": {"pseudo_stream": {"updated": "2025-12-18T11:28:40Z"}}}
        new_value = "2025-12-18T11:28:44Z"
        
        result = self.stream._write_bookmark(state, new_value)
        
        mock_write_bookmark.assert_called_once_with(state, "pseudo_stream", "updated", new_value)
        mock_write_state.assert_called_once()

    @patch("tap_quickbase.streams.abstracts.write_state")
    @patch("tap_quickbase.streams.abstracts.write_bookmark")
    @patch("tap_quickbase.streams.abstracts.get_bookmark")
    def test_write_bookmark_skips_older_value(self, mock_get_bookmark, mock_write_bookmark, mock_write_state):
        """Test bookmark is NOT written when new value is older."""
        mock_get_bookmark.return_value = "2025-12-18T11:28:44Z"
        
        state = {"bookmarks": {"pseudo_stream": {"updated": "2025-12-18T11:28:44Z"}}}
        new_value = "2025-12-18T11:28:40Z"
        
        result = self.stream._write_bookmark(state, new_value)
        
        # Should not call write_bookmark because new value is older
        mock_write_bookmark.assert_not_called()
        mock_write_state.assert_not_called()

    @patch("tap_quickbase.streams.abstracts.write_state")
    @patch("tap_quickbase.streams.abstracts.write_record")
    @patch("tap_quickbase.streams.abstracts.get_bookmark")
    @patch("tap_quickbase.streams.abstracts.metadata")
    def test_sync_filters_records_less_than_bookmark(self, mock_metadata, mock_get_bookmark, mock_write_record, mock_write_state):
        """Test records with updated < bookmark are filtered out."""
        mock_get_bookmark.return_value = "2025-12-18T11:28:44Z"
        mock_metadata.get.return_value = True
        
        state = {"bookmarks": {"pseudo_stream": {"updated": "2025-12-18T11:28:44Z"}}}
        
        # Mock records: one before bookmark, one after
        self.stream.get_records = MagicMock(return_value=[
            {"id": "1", "updated": "2025-12-18T11:28:40Z"},  # Before bookmark - filtered
            {"id": "2", "updated": "2025-12-18T11:28:50Z"},  # After bookmark - emitted
        ])
        
        transformer = MagicMock()
        transformer.transform.side_effect = lambda rec, schema, metadata: rec
        self.stream.url_endpoint = "http://test.com"
        
        result = self.stream.sync(state, transformer)
        
        # Only record after bookmark should be written
        self.assertEqual(mock_write_record.call_count, 1)
        self.assertEqual(mock_write_record.call_args[0][1]["id"], "2")

    @patch("tap_quickbase.streams.abstracts.write_state")
    @patch("tap_quickbase.streams.abstracts.write_record")
    @patch("tap_quickbase.streams.abstracts.get_bookmark")
    @patch("tap_quickbase.streams.abstracts.metadata")
    def test_sync_filters_records_equal_to_bookmark_minus_one_second(self, mock_metadata, mock_get_bookmark, mock_write_record, mock_write_state):
        """Test records at (bookmark - 1 second) are filtered out."""
        mock_get_bookmark.return_value = "2025-12-18T11:28:44Z"
        mock_metadata.get.return_value = True
        
        state = {"bookmarks": {"pseudo_stream": {"updated": "2025-12-18T11:28:44Z"}}}
        
        # Record at bookmark - 1 second (will be <= filter threshold)
        self.stream.get_records = MagicMock(return_value=[
            {"id": "1", "updated": "2025-12-18T11:28:43Z"},  # At threshold - filtered
        ])
        
        transformer = MagicMock()
        transformer.transform.side_effect = lambda rec, schema, metadata: rec
        self.stream.url_endpoint = "http://test.com"
        
        result = self.stream.sync(state, transformer)
        
        # Record at threshold should be filtered out
        mock_write_record.assert_not_called()

    @patch("tap_quickbase.streams.abstracts.write_state")
    @patch("tap_quickbase.streams.abstracts.write_record")
    @patch("tap_quickbase.streams.abstracts.get_bookmark")
    @patch("tap_quickbase.streams.abstracts.metadata")
    def test_sync_emits_records_greater_than_bookmark(self, mock_metadata, mock_get_bookmark, mock_write_record, mock_write_state):
        """Test records with updated > bookmark are emitted."""
        mock_get_bookmark.return_value = "2025-12-18T11:28:44Z"
        mock_metadata.get.return_value = True
        
        state = {"bookmarks": {"pseudo_stream": {"updated": "2025-12-18T11:28:44Z"}}}
        
        # Records after bookmark
        self.stream.get_records = MagicMock(return_value=[
            {"id": "1", "updated": "2025-12-18T11:28:45Z"},
            {"id": "2", "updated": "2025-12-18T11:29:00Z"},
            {"id": "3", "updated": "2025-12-18T12:00:00Z"},
        ])
        
        transformer = MagicMock()
        transformer.transform.side_effect = lambda rec, schema, metadata: rec
        self.stream.url_endpoint = "http://test.com"
        
        result = self.stream.sync(state, transformer)
        
        # All records after bookmark should be written
        self.assertEqual(mock_write_record.call_count, 3)

    @patch("tap_quickbase.streams.abstracts.write_state")
    @patch("tap_quickbase.streams.abstracts.write_record")
    @patch("tap_quickbase.streams.abstracts.get_bookmark")
    @patch("tap_quickbase.streams.abstracts.metadata")
    def test_sync_one_second_overlap_ensures_resync_emits_records(self, mock_metadata, mock_get_bookmark, mock_write_record, mock_write_state):
        """
        Test 1-second overlap ensures re-sync with same state emits records.
        
        This is the key test demonstrating that using the same state file
        for a second sync will still emit records at the bookmark timestamp.
        
        Bookmark: 2025-12-18T11:28:44Z
        Filter:   2025-12-18T11:28:43Z (bookmark - 1 second)
        Result:   Records at 11:28:44Z are re-emitted
        """
        mock_get_bookmark.return_value = "2025-12-18T11:28:44Z"
        mock_metadata.get.return_value = True
        
        state = {"bookmarks": {"pseudo_stream": {"updated": "2025-12-18T11:28:44Z"}}}
        
        self.stream.get_records = MagicMock(return_value=[
            {"id": "1", "updated": "2025-12-18T11:28:43Z"},  # At threshold - filtered
            {"id": "2", "updated": "2025-12-18T11:28:44Z"},  # At bookmark - EMITTED (overlap)
            {"id": "3", "updated": "2025-12-18T11:28:45Z"},  # After bookmark - emitted
        ])
        
        transformer = MagicMock()
        transformer.transform.side_effect = lambda rec, schema, metadata: rec
        self.stream.url_endpoint = "http://test.com"
        
        result = self.stream.sync(state, transformer)
        
        # Two records should be written (id 2 and 3)
        # This ensures re-sync with same state produces output
        self.assertEqual(mock_write_record.call_count, 2)
        
        # Verify correct records were written
        written_ids = [call[0][1]["id"] for call in mock_write_record.call_args_list]
        self.assertIn("2", written_ids)  # Bookmark timestamp record re-emitted
        self.assertIn("3", written_ids)
        self.assertNotIn("1", written_ids)

    @patch("tap_quickbase.streams.abstracts.write_state")
    @patch("tap_quickbase.streams.abstracts.write_record")
    @patch("tap_quickbase.streams.abstracts.get_bookmark")
    @patch("tap_quickbase.streams.abstracts.metadata")
    def test_sync_emits_records_between_threshold_and_bookmark(self, mock_metadata, mock_get_bookmark, mock_write_record, mock_write_state):
        """
        Test records with updated BETWEEN (bookmark - 1 second) and bookmark are emitted.
        
        This tests the overlap window more precisely:
        Bookmark stored: 2025-12-18T11:28:44Z
        Filter threshold: 2025-12-18T11:28:43Z (bookmark - 1 second)
        
        Records with updated > 11:28:43Z AND <= 11:28:44Z should be emitted.
        These are records LESS than the stored bookmark but within the 1-second overlap.
        """
        mock_get_bookmark.return_value = "2025-12-18T11:28:44Z"
        mock_metadata.get.return_value = True
        
        state = {"bookmarks": {"pseudo_stream": {"updated": "2025-12-18T11:28:44Z"}}}
        
        # Test records at various points in the 1-second overlap window
        self.stream.get_records = MagicMock(return_value=[
            {"id": "1", "updated": "2025-12-18T11:28:42.999Z"},  # Before threshold - filtered
            {"id": "2", "updated": "2025-12-18T11:28:43.000Z"},  # Exactly at threshold - filtered
            {"id": "3", "updated": "2025-12-18T11:28:43.100Z"},  # 0.1s after threshold - EMITTED
            {"id": "4", "updated": "2025-12-18T11:28:43.500Z"},  # 0.5s after threshold - EMITTED
            {"id": "5", "updated": "2025-12-18T11:28:43.999Z"},  # 0.999s after threshold - EMITTED
            {"id": "6", "updated": "2025-12-18T11:28:44.000Z"},  # Exactly at bookmark - EMITTED
            {"id": "7", "updated": "2025-12-18T11:28:45.000Z"},  # After bookmark - EMITTED
        ])
        
        transformer = MagicMock()
        transformer.transform.side_effect = lambda rec, schema, metadata: rec
        self.stream.url_endpoint = "http://test.com"
        
        result = self.stream.sync(state, transformer)
        
        # Should write 5 records (ids 3, 4, 5, 6, 7)
        # Records 1 and 2 should be filtered
        self.assertEqual(mock_write_record.call_count, 5)
        
        # Verify correct records were written
        written_ids = [call[0][1]["id"] for call in mock_write_record.call_args_list]
        self.assertNotIn("1", written_ids)  # Before threshold
        self.assertNotIn("2", written_ids)  # At threshold
        self.assertIn("3", written_ids)     # In overlap window
        self.assertIn("4", written_ids)     # In overlap window
        self.assertIn("5", written_ids)     # In overlap window
        self.assertIn("6", written_ids)     # At bookmark
        self.assertIn("7", written_ids)     # After bookmark

