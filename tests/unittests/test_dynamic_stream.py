"""Unit tests for DynamicTableStream sync behaviour."""

import json
import unittest
from unittest.mock import MagicMock, call, patch

from singer import metadata as singer_metadata

from tap_quickbase.dynamic_schema import (
    build_metadata_for_dynamic_stream,
    build_schema_from_fields,
)
from tap_quickbase.streams.dynamic import DynamicTableStream


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_FIELDS = [
    {"id": 2, "label": "Date Modified", "fieldType": "timestamp"},
    {"id": 3, "label": "Record ID#",    "fieldType": "recordid"},
    {"id": 6, "label": "Name",          "fieldType": "text"},
    {"id": 7, "label": "Status",        "fieldType": "text"},
]


def _make_catalog_entry(
    stream_name="myapp__orders",
    table_id="t1",
    fields=None,
    selected=True,
):
    """Build a mock CatalogEntry that DynamicTableStream can consume."""
    fields = fields if fields is not None else SAMPLE_FIELDS
    schema, fid_map, repl_key, key_props = build_schema_from_fields(fields)
    mdata_list = build_metadata_for_dynamic_stream(
        schema=schema,
        key_properties=key_props,
        replication_key=repl_key,
        table_id=table_id,
        field_id_to_name=fid_map,
    )

    # Mark all properties as selected
    mmap = singer_metadata.to_map(mdata_list)
    mmap = singer_metadata.write(mmap, (), "selected", selected)
    for prop in schema.get("properties", {}):
        mmap = singer_metadata.write(mmap, ("properties", prop), "selected", True)
    mdata_list = singer_metadata.to_list(mmap)

    catalog_entry = MagicMock()
    catalog_entry.tap_stream_id = stream_name
    catalog_entry.schema.to_dict.return_value = schema
    catalog_entry.metadata = mdata_list
    return catalog_entry


def _make_client(records_pages=None, config=None):
    """Build a mock Client that serves paginated records."""
    records_pages = records_pages or [[]]
    config = config or {"start_date": "2023-01-01T00:00:00Z", "page_size": 100}
    client = MagicMock()
    client.base_url = "https://api.quickbase.com"
    client.config = config

    responses = iter(
        [{"data": page, "metadata": {"totalRecords": sum(len(p) for p in records_pages)}}
         for page in records_pages]
    )

    def make_request(*args, **kwargs):
        return next(responses)

    client.make_request.side_effect = make_request
    return client


def _make_raw_record(rid=1, name="Alice", status="open", date_modified="2023-06-01T00:00:00Z"):
    """Create a raw QB API record dict (field-ID keyed)."""
    return {
        "3": {"value": rid},
        "6": {"value": name},
        "7": {"value": status},
        "2": {"value": date_modified},
    }


# ---------------------------------------------------------------------------
# DynamicTableStream construction
# ---------------------------------------------------------------------------

class TestDynamicTableStreamInit(unittest.TestCase):

    def setUp(self):
        self.entry = _make_catalog_entry()
        self.client = _make_client()
        self.stream = DynamicTableStream(self.client, self.entry)

    def test_tap_stream_id_set(self):
        self.assertEqual(self.stream.tap_stream_id, "myapp__orders")

    def test_table_id_set(self):
        self.assertEqual(self.stream.table_id, "t1")

    def test_replication_method_incremental(self):
        self.assertEqual(self.stream.replication_method, "INCREMENTAL")

    def test_replication_keys_populated(self):
        self.assertTrue(len(self.stream.replication_keys) > 0)

    def test_date_modified_field_id_resolved(self):
        self.assertEqual(self.stream.date_modified_field_id, 2)

    def test_key_properties_contains_record_id(self):
        # The field_id_to_name for field 3 is "record_id"
        record_id_name = self.stream.field_id_to_name.get("3")
        self.assertIn(record_id_name, self.stream.key_properties)

    def test_selected_field_ids_populated(self):
        self.assertTrue(len(self.stream.selected_field_ids) > 0)
        # Must include field IDs 2, 3, 6, 7
        self.assertIn(2, self.stream.selected_field_ids)
        self.assertIn(3, self.stream.selected_field_ids)

    def test_no_children(self):
        self.assertEqual(self.stream.children, [])

    def test_no_parent(self):
        self.assertEqual(self.stream.parent, "")


# ---------------------------------------------------------------------------
# DynamicTableStream bookmark helpers
# ---------------------------------------------------------------------------

class TestDynamicTableStreamBookmarks(unittest.TestCase):

    def setUp(self):
        self.entry = _make_catalog_entry()
        self.client = _make_client()
        self.stream = DynamicTableStream(self.client, self.entry)

    def test_get_bookmark_returns_start_date_when_no_bookmark(self):
        state = {}
        bookmark = self.stream.get_bookmark(state, self.stream.tap_stream_id)
        self.assertEqual(bookmark, "2023-01-01T00:00:00Z")

    def test_get_bookmark_returns_existing_bookmark(self):
        repl_key = self.stream.replication_keys[0]
        state = {"bookmarks": {self.stream.tap_stream_id: {repl_key: "2023-06-01T00:00:00Z"}}}
        bookmark = self.stream.get_bookmark(state, self.stream.tap_stream_id)
        self.assertEqual(bookmark, "2023-06-01T00:00:00Z")

    def test_write_bookmark_sets_value(self):
        state = {}
        new_state = self.stream.write_bookmark(
            state, self.stream.tap_stream_id, value="2023-09-01T00:00:00Z"
        )
        repl_key = self.stream.replication_keys[0]
        self.assertEqual(
            new_state["bookmarks"][self.stream.tap_stream_id][repl_key],
            "2023-09-01T00:00:00Z",
        )

    def test_write_bookmark_keeps_max_value(self):
        repl_key = self.stream.replication_keys[0]
        state = {"bookmarks": {self.stream.tap_stream_id: {repl_key: "2023-09-01T00:00:00Z"}}}
        # Try to write an earlier value – should not regress
        new_state = self.stream.write_bookmark(
            state, self.stream.tap_stream_id, value="2023-01-01T00:00:00Z"
        )
        self.assertEqual(
            new_state["bookmarks"][self.stream.tap_stream_id][repl_key],
            "2023-09-01T00:00:00Z",
        )

    def test_write_bookmark_advances_when_newer(self):
        repl_key = self.stream.replication_keys[0]
        state = {"bookmarks": {self.stream.tap_stream_id: {repl_key: "2023-01-01T00:00:00Z"}}}
        new_state = self.stream.write_bookmark(
            state, self.stream.tap_stream_id, value="2024-01-01T00:00:00Z"
        )
        self.assertEqual(
            new_state["bookmarks"][self.stream.tap_stream_id][repl_key],
            "2024-01-01T00:00:00Z",
        )


# ---------------------------------------------------------------------------
# DynamicTableStream._to_epoch_ms
# ---------------------------------------------------------------------------

class TestToEpochMs(unittest.TestCase):

    def test_converts_iso_string(self):
        val = DynamicTableStream._to_epoch_ms("2023-01-01T00:00:00Z")
        self.assertIsInstance(val, int)
        self.assertGreater(val, 0)

    def test_invalid_string_returns_zero(self):
        val = DynamicTableStream._to_epoch_ms("not-a-date")
        self.assertEqual(val, 0)

    def test_empty_string_returns_zero(self):
        val = DynamicTableStream._to_epoch_ms("")
        self.assertEqual(val, 0)


# ---------------------------------------------------------------------------
# DynamicTableStream._convert_raw_record
# ---------------------------------------------------------------------------

class TestConvertRawRecord(unittest.TestCase):

    def setUp(self):
        self.entry = _make_catalog_entry()
        self.client = _make_client()
        self.stream = DynamicTableStream(self.client, self.entry)

    def test_converts_field_ids_to_names(self):
        raw = _make_raw_record(rid=42, name="Bob")
        record = self.stream._convert_raw_record(raw)
        # Field 6 → "name", field 3 → "record_id" (or similar sanitized name)
        name_field = self.stream.field_id_to_name["6"]
        self.assertEqual(record[name_field], "Bob")

    def test_extracts_value_from_dict(self):
        raw = {"6": {"value": "Alice"}}
        record = self.stream._convert_raw_record(raw)
        self.assertEqual(record.get(self.stream.field_id_to_name.get("6")), "Alice")

    def test_bare_scalar_handled(self):
        raw = {"6": "Alice"}
        record = self.stream._convert_raw_record(raw)
        self.assertEqual(record.get(self.stream.field_id_to_name.get("6")), "Alice")

    def test_unknown_field_id_gets_generic_name(self):
        raw = {"999": {"value": "x"}}
        record = self.stream._convert_raw_record(raw)
        self.assertIn("field_999", record)


# ---------------------------------------------------------------------------
# DynamicTableStream.get_records
# ---------------------------------------------------------------------------

class TestGetRecords(unittest.TestCase):

    def test_empty_response_yields_nothing(self):
        entry = _make_catalog_entry()
        client = _make_client(records_pages=[[]])
        stream = DynamicTableStream(client, entry)
        records = list(stream.get_records())
        self.assertEqual(records, [])

    def test_single_page_yields_all_records(self):
        entry = _make_catalog_entry()
        page = [_make_raw_record(i) for i in range(1, 6)]
        client = _make_client(records_pages=[page])
        stream = DynamicTableStream(client, entry)
        records = list(stream.get_records())
        self.assertEqual(len(records), 5)

    def test_multi_page_yields_all_records(self):
        entry = _make_catalog_entry()
        page1 = [_make_raw_record(i) for i in range(1, 4)]
        page2 = [_make_raw_record(i) for i in range(4, 7)]
        page3 = []  # terminal empty page

        client = _make_client(
            records_pages=[page1, page2, page3],
            config={"start_date": "2023-01-01T00:00:00Z", "page_size": 3},
        )
        # Adjust terminal-page logic: page2 < page_size → stops after page2
        responses = iter([
            {"data": page1, "metadata": {"totalRecords": 6}},
            {"data": page2, "metadata": {"totalRecords": 6}},
        ])
        client.make_request.side_effect = lambda *a, **kw: next(responses)

        stream = DynamicTableStream(client, entry)
        records = list(stream.get_records())
        # page1=3 + page2=3 = 6, then page2 size == page_size so one more call, then empty
        self.assertGreaterEqual(len(records), 6)

    def test_api_error_stops_iteration(self):
        entry = _make_catalog_entry()
        client = MagicMock()
        client.base_url = "https://api.quickbase.com"
        client.config = {"start_date": "2023-01-01T00:00:00Z", "page_size": 100}
        client.make_request.side_effect = Exception("Network error")
        stream = DynamicTableStream(client, entry)
        # Should not raise, just yield nothing
        records = list(stream.get_records())
        self.assertEqual(records, [])

    def test_query_body_contains_table_id(self):
        entry = _make_catalog_entry()
        client = _make_client(records_pages=[[]])
        stream = DynamicTableStream(client, entry)
        list(stream.get_records())
        call_args = client.make_request.call_args
        body_str = call_args[1].get("body") or call_args[0][2] if len(call_args[0]) > 2 else None
        if body_str is None:
            # keyword 'body'
            body_str = call_args.kwargs.get("body")
        body = json.loads(body_str) if body_str else {}
        self.assertEqual(body.get("from"), "t1")

    def test_incremental_query_contains_where(self):
        entry = _make_catalog_entry()
        client = _make_client(records_pages=[[]])
        stream = DynamicTableStream(client, entry)
        stream._bookmark_epoch_ms = 1000000  # simulate a bookmark
        list(stream.get_records())
        call_args = client.make_request.call_args
        body_str = call_args.kwargs.get("body") or (
            call_args[0][2] if len(call_args[0]) > 2 else "{}"
        )
        body = json.loads(body_str) if body_str else {}
        self.assertIn("where", body)
        self.assertIn("1000000", body["where"])


# ---------------------------------------------------------------------------
# DynamicTableStream.sync
# ---------------------------------------------------------------------------

class TestDynamicTableStreamSync(unittest.TestCase):

    def _run_sync(self, fields=None, pages=None, state=None):
        """Helper: run sync and return (records_written, state)."""
        import io
        import singer

        fields = fields if fields is not None else SAMPLE_FIELDS
        pages = pages or [[]]
        state = state or {}

        entry = _make_catalog_entry(fields=fields)

        responses = iter([
            {"data": page, "metadata": {"totalRecords": sum(len(p) for p in pages)}}
            for page in pages
        ])
        client = MagicMock()
        client.base_url = "https://api.quickbase.com"
        client.config = {"start_date": "2023-01-01T00:00:00Z", "page_size": 100}
        client.make_request.side_effect = lambda *a, **kw: next(responses)

        stream = DynamicTableStream(client, entry)

        written_records = []

        def fake_write_record(stream_id, record, **kwargs):
            written_records.append(record)

        with patch("tap_quickbase.streams.dynamic.write_record", side_effect=fake_write_record), \
             patch("tap_quickbase.streams.dynamic.write_state"):
            transformer = singer.Transformer()
            count = stream.sync(state=state, transformer=transformer)

        return count, state, written_records

    def test_sync_returns_record_count(self):
        pages = [[_make_raw_record(1), _make_raw_record(2)]]
        count, _, _ = self._run_sync(pages=pages)
        self.assertEqual(count, 2)

    def test_sync_writes_records(self):
        pages = [[_make_raw_record(1), _make_raw_record(2)]]
        _, _, records = self._run_sync(pages=pages)
        self.assertEqual(len(records), 2)

    def test_sync_updates_bookmark(self):
        pages = [[
            _make_raw_record(1, date_modified="2023-06-01T00:00:00Z"),
            _make_raw_record(2, date_modified="2023-09-01T00:00:00Z"),
        ]]
        _, state, _ = self._run_sync(pages=pages, state={})
        repl_key = DynamicTableStream(
            _make_client(), _make_catalog_entry()
        ).replication_keys[0]
        bookmarks = state.get("bookmarks", {}).get("myapp__orders", {})
        self.assertIn(repl_key, bookmarks)
        # singer Transformer may reformat to 'YYYY-MM-DDTHH:MM:SS.000000Z'
        self.assertTrue(
            bookmarks[repl_key].startswith("2023-09-01T00:00:00"),
            f"Expected bookmark starting with 2023-09-01T00:00:00, got {bookmarks[repl_key]}"
        )

    def test_sync_respects_existing_bookmark(self):
        """Records older than bookmark should be filtered out."""
        repl_key = DynamicTableStream(
            _make_client(), _make_catalog_entry()
        ).replication_keys[0]
        state = {"bookmarks": {"myapp__orders": {repl_key: "2023-07-01T00:00:00Z"}}}

        pages = [[
            _make_raw_record(1, date_modified="2023-05-01T00:00:00Z"),  # older – skip
            _make_raw_record(2, date_modified="2023-08-01T00:00:00Z"),  # newer – emit
        ]]
        _, _, records = self._run_sync(pages=pages, state=state)
        # Only the newer record passes tap-side filter
        self.assertEqual(len(records), 1)

    def test_sync_no_replication_key_full_table(self):
        """Full table stream: all records emitted, no bookmark written."""
        fields_no_ts = [
            {"id": 3, "label": "Record ID#", "fieldType": "recordid"},
            {"id": 6, "label": "Name",       "fieldType": "text"},
        ]
        # Only use field IDs 3 and 6 to match the schema (no extra fields)
        pages = [[
            {"3": {"value": 1}, "6": {"value": "Alice"}},
            {"3": {"value": 2}, "6": {"value": "Bob"}},
        ]]
        count, state, records = self._run_sync(fields=fields_no_ts, pages=pages)
        self.assertEqual(count, 2)
        # No bookmark entries expected (no replication key)
        self.assertNotIn("bookmarks", state)

    def test_sync_empty_table(self):
        count, _, records = self._run_sync(pages=[[]])
        self.assertEqual(count, 0)
        self.assertEqual(records, [])


# ---------------------------------------------------------------------------
# DynamicTableStream.write_schema
# ---------------------------------------------------------------------------

class TestWriteSchema(unittest.TestCase):

    def test_write_schema_calls_singer(self):
        entry = _make_catalog_entry()
        client = _make_client()
        stream = DynamicTableStream(client, entry)

        with patch("tap_quickbase.streams.dynamic.write_record"), \
             patch("tap_quickbase.streams.abstracts.write_schema") as mock_ws:
            stream.write_schema()
            mock_ws.assert_called_once()


if __name__ == "__main__":
    unittest.main()
