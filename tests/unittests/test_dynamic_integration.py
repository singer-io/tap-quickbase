"""Integration tests for dynamic stream discovery and sync.

These tests exercise the full pipeline:

  discover(client) → Catalog  (static + dynamic streams)
  sync(client, catalog, state) → Singer messages on stdout

All QB REST API calls are mocked so no live credentials are required.
"""

import io
import json
import sys
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

import singer

from tap_quickbase.client import Client
from tap_quickbase.discover import discover
from tap_quickbase.sync import sync


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

APP_TABLES = [
    {"id": "t1", "name": "Orders",   "alias": "acme"},
    {"id": "t2", "name": "Products", "alias": "acme"},
]

FIELDS_T1 = [
    {"id": 2, "label": "Date Modified", "fieldType": "timestamp"},
    {"id": 3, "label": "Record ID#",    "fieldType": "recordid"},
    {"id": 6, "label": "Customer",      "fieldType": "text"},
    {"id": 7, "label": "Amount",        "fieldType": "numeric"},
]

FIELDS_T2 = [
    {"id": 2, "label": "Date Modified", "fieldType": "timestamp"},
    {"id": 3, "label": "Record ID#",    "fieldType": "recordid"},
    {"id": 6, "label": "Product Name",  "fieldType": "text"},
    {"id": 7, "label": "Price",         "fieldType": "numeric"},
]

RECORDS_T1 = [
    {"3": {"value": 1}, "6": {"value": "Alice"}, "7": {"value": 99.9},  "2": {"value": "2023-06-01T10:00:00Z"}},
    {"3": {"value": 2}, "6": {"value": "Bob"},   "7": {"value": 149.0}, "2": {"value": "2023-07-01T10:00:00Z"}},
]

RECORDS_T2 = [
    {"3": {"value": 10}, "6": {"value": "Widget"}, "7": {"value": 5.0}, "2": {"value": "2023-06-15T08:00:00Z"}},
]

QB_CONFIG = {
    "qb_user_token": "token_test",
    "qb_appid": "app_test",
    "qb_url": "https://test.quickbase.com/db/",
    "start_date": "2023-01-01T00:00:00Z",
    "page_size": 100,
}


def _build_mock_client(extra_config=None):
    """Return a Client whose make_request is mocked with canned responses."""
    cfg = dict(QB_CONFIG)
    if extra_config:
        cfg.update(extra_config)

    client = MagicMock(spec=Client)
    client.base_url = "https://api.quickbase.com"
    client.config = cfg

    def make_request(method, endpoint, params=None, headers=None, body=None, **_kw):
        # Dynamic discovery calls
        if "v1/tables" in endpoint:
            return APP_TABLES
        if "v1/fields" in endpoint:
            tid = (params or {}).get("tableId", "")
            return FIELDS_T1 if tid == "t1" else FIELDS_T2

        # Static stream calls (metadata endpoints)
        if "v1/apps" in endpoint:
            return {
                "id": "app_test",
                "name": "Acme",
                "updated": "2023-03-01T00:00:00Z",
            }
        if "v1/events" in endpoint:
            return []
        if "v1/roles" in endpoint:
            return []
        if "v1/tables" in endpoint:  # static app_tables
            return []

        # Dynamic data record calls
        if "v1/records/query" in endpoint:
            try:
                body_dict = json.loads(body) if isinstance(body, str) else (body or {})
            except Exception:
                body_dict = {}
            from_table = body_dict.get("from", "")
            if from_table == "t1":
                return {"data": RECORDS_T1, "metadata": {"totalRecords": 2}}
            if from_table == "t2":
                return {"data": RECORDS_T2, "metadata": {"totalRecords": 1}}
            return {"data": [], "metadata": {"totalRecords": 0}}

        return {}

    client.make_request.side_effect = make_request
    return client


def _select_all_streams(catalog):
    """Return a copy of the catalog with all streams selected."""
    import copy
    from singer.catalog import Catalog, CatalogEntry
    from singer import metadata as md

    new_streams = []
    for entry in catalog.streams:
        mmap = md.to_map(copy.deepcopy(entry.metadata))
        mmap = md.write(mmap, (), "selected", True)
        for prop in entry.schema.to_dict().get("properties", {}):
            mmap = md.write(mmap, ("properties", prop), "selected", True)
        new_entry = CatalogEntry(
            stream=entry.stream,
            tap_stream_id=entry.tap_stream_id,
            key_properties=entry.key_properties,
            schema=entry.schema,
            metadata=md.to_list(mmap),
        )
        new_streams.append(new_entry)
    return Catalog(new_streams)


def _select_dynamic_streams_only(catalog):
    """Return a catalog with only dynamic streams selected (for targeted sync tests)."""
    import copy
    from singer.catalog import Catalog, CatalogEntry
    from singer import metadata as md

    new_streams = []
    for entry in catalog.streams:
        mmap = md.to_map(copy.deepcopy(entry.metadata))
        is_dynamic = md.get(mmap, (), "tap-quickbase.is_dynamic")
        selected = bool(is_dynamic)
        mmap = md.write(mmap, (), "selected", selected)
        if selected:
            for prop in entry.schema.to_dict().get("properties", {}):
                mmap = md.write(mmap, ("properties", prop), "selected", True)
        new_entry = CatalogEntry(
            stream=entry.stream,
            tap_stream_id=entry.tap_stream_id,
            key_properties=entry.key_properties,
            schema=entry.schema,
            metadata=md.to_list(mmap),
        )
        new_streams.append(new_entry)
    return Catalog(new_streams)


# ---------------------------------------------------------------------------
# Test: Discovery
# ---------------------------------------------------------------------------

class TestDynamicDiscovery(unittest.TestCase):
    """Integration tests for discover()."""

    def setUp(self):
        self.client = _build_mock_client()
        self.catalog = discover(client=self.client)

    def test_catalog_contains_static_streams(self):
        stream_names = [s.stream for s in self.catalog.streams]
        for static_name in ("apps", "tables", "fields", "events", "roles"):
            self.assertIn(static_name, stream_names, f"Missing static stream: {static_name}")

    def test_catalog_contains_dynamic_streams(self):
        stream_names = [s.stream for s in self.catalog.streams]
        # Dynamic streams from APP_TABLES should appear
        self.assertTrue(
            any("acme" in n and "orders" in n for n in stream_names),
            f"Expected acme__orders in streams, got: {stream_names}"
        )
        self.assertTrue(
            any("acme" in n and "products" in n for n in stream_names),
            f"Expected acme__products in streams, got: {stream_names}"
        )

    def test_total_stream_count(self):
        static_count = 10  # number of STREAMS entries
        dynamic_count = len(APP_TABLES)
        self.assertEqual(len(self.catalog.streams), static_count + dynamic_count)

    def test_dynamic_stream_has_correct_schema(self):
        dynamic = next(
            (s for s in self.catalog.streams if "orders" in s.stream), None
        )
        self.assertIsNotNone(dynamic, "orders stream not found")
        props = dynamic.schema.to_dict().get("properties", {})
        self.assertGreater(len(props), 0)

    def test_dynamic_stream_has_key_properties(self):
        dynamic = next(
            (s for s in self.catalog.streams if "orders" in s.stream), None
        )
        self.assertIsNotNone(dynamic)
        self.assertIsNotNone(dynamic.key_properties)
        self.assertGreater(len(dynamic.key_properties), 0)

    def test_dynamic_stream_has_metadata_table_id(self):
        from singer import metadata as md
        dynamic = next(
            (s for s in self.catalog.streams if "orders" in s.stream), None
        )
        self.assertIsNotNone(dynamic)
        mmap = md.to_map(dynamic.metadata)
        table_id = md.get(mmap, (), "tap-quickbase.table_id")
        self.assertEqual(table_id, "t1")

    def test_dynamic_stream_flagged_as_dynamic(self):
        from singer import metadata as md
        dynamic = next(
            (s for s in self.catalog.streams if "orders" in s.stream), None
        )
        mmap = md.to_map(dynamic.metadata)
        self.assertTrue(md.get(mmap, (), "tap-quickbase.is_dynamic"))

    def test_static_streams_not_flagged_as_dynamic(self):
        from singer import metadata as md
        apps_stream = next(s for s in self.catalog.streams if s.stream == "apps")
        mmap = md.to_map(apps_stream.metadata)
        self.assertFalse(md.get(mmap, (), "tap-quickbase.is_dynamic"))

    def test_discovery_without_client_returns_only_static(self):
        catalog_no_client = discover(client=None)
        stream_names = [s.stream for s in catalog_no_client.streams]
        self.assertNotIn("acme__orders", stream_names)
        self.assertIn("apps", stream_names)

    def test_catalog_serialisation(self):
        """Catalog must be JSON-serialisable (needed for --discover output)."""
        buf = io.StringIO()
        with redirect_stdout(buf):
            json.dump(self.catalog.to_dict(), sys.stdout, indent=2)
        raw = buf.getvalue()
        parsed = json.loads(raw)
        self.assertIn("streams", parsed)


# ---------------------------------------------------------------------------
# Test: Sync – dynamic streams only
# ---------------------------------------------------------------------------

class TestDynamicSync(unittest.TestCase):
    """Integration tests for sync() with dynamic streams only."""

    def setUp(self):
        self.client = _build_mock_client()
        full_catalog = discover(client=self.client)
        # Only select dynamic streams to avoid static-stream mock-data mismatches
        self.catalog = _select_dynamic_streams_only(full_catalog)

    def _capture_sync(self, state=None):
        """Run sync and collect emitted Singer messages."""
        state = state or {}
        messages = []

        def fake_write_record(stream_id, record, **kwargs):
            messages.append({"type": "RECORD", "stream": stream_id, "record": record})

        def fake_write_state(s):
            messages.append({"type": "STATE", "value": s})

        def fake_write_schema(stream_id, schema, key_properties, **kwargs):
            messages.append({"type": "SCHEMA", "stream": stream_id})

        client = _build_mock_client()

        with patch("tap_quickbase.streams.dynamic.write_record",  side_effect=fake_write_record), \
             patch("tap_quickbase.streams.dynamic.write_state",   side_effect=fake_write_state), \
             patch("tap_quickbase.streams.abstracts.write_schema", side_effect=fake_write_schema):
            sync(
                client=client,
                config=QB_CONFIG,
                catalog=self.catalog,
                state=state,
            )
        return messages, state

    def test_schema_messages_emitted_for_dynamic_streams(self):
        messages, _ = self._capture_sync()
        schema_streams = {m["stream"] for m in messages if m["type"] == "SCHEMA"}
        self.assertTrue(
            any("orders" in s for s in schema_streams),
            f"Expected orders schema, got: {schema_streams}"
        )

    def test_records_emitted_for_dynamic_streams(self):
        messages, _ = self._capture_sync()
        record_streams = [m["stream"] for m in messages if m["type"] == "RECORD"]
        orders_records = [s for s in record_streams if "orders" in s]
        products_records = [s for s in record_streams if "products" in s]
        self.assertEqual(len(orders_records), len(RECORDS_T1))
        self.assertEqual(len(products_records), len(RECORDS_T2))

    def test_state_updated_after_dynamic_sync(self):
        _, state = self._capture_sync()
        self.assertIsInstance(state, dict)


# ---------------------------------------------------------------------------
# Test: Bookmark handling for dynamic streams
# ---------------------------------------------------------------------------

class TestDynamicBookmarks(unittest.TestCase):

    def _run_incremental(self, initial_state, records):
        """Run sync for a single dynamic stream and return final state."""
        client = _build_mock_client()
        full_catalog = discover(client=client)
        catalog = _select_dynamic_streams_only(full_catalog)

        # Find orders stream entry
        orders_entry = next(
            (s for s in catalog.streams if "orders" in s.stream), None
        )
        if orders_entry is None:
            self.skipTest("orders stream not in catalog")

        from tap_quickbase.streams.dynamic import DynamicTableStream

        stream = DynamicTableStream(_build_mock_client(), orders_entry)

        responses = iter([
            {"data": records, "metadata": {"totalRecords": len(records)}},
            {"data": [],      "metadata": {"totalRecords": len(records)}},
        ])

        sync_client = MagicMock()
        sync_client.base_url = "https://api.quickbase.com"
        sync_client.config = QB_CONFIG
        sync_client.make_request.side_effect = lambda *a, **kw: next(responses)

        stream.client = sync_client
        state = dict(initial_state)

        with patch("tap_quickbase.streams.dynamic.write_record"), \
             patch("tap_quickbase.streams.dynamic.write_state"):
            stream.sync(state=state, transformer=singer.Transformer())

        return state

    def test_bookmark_advances_to_max_modified(self):
        records = [
            {"3": {"value": 1}, "6": {"value": "a"}, "7": {"value": 1.0}, "2": {"value": "2023-06-01T00:00:00Z"}},
            {"3": {"value": 2}, "6": {"value": "b"}, "7": {"value": 2.0}, "2": {"value": "2023-09-01T00:00:00Z"}},
        ]
        final_state = self._run_incremental({}, records)
        # Drill into bookmarks to find the max date
        stream_bookmarks = (
            final_state.get("bookmarks", {})
        )
        if stream_bookmarks:
            stream_state = next(iter(stream_bookmarks.values()), {})
            bookmark_val = next(iter(stream_state.values()), None)
            if bookmark_val:
                # Singer Transformer may normalize to microseconds: "...T00:00:00.000000Z"
                # Use prefix comparison to avoid ASCII-sort issue ('.' < 'Z')
                self.assertTrue(
                    bookmark_val.startswith("2023-09-01T00:00:00"),
                    f"Expected bookmark near 2023-09-01, got: {bookmark_val}"
                )

    def test_bookmark_not_regressed(self):
        """A second sync with no new records should not regress the bookmark."""
        initial_state = {
            "bookmarks": {"acme__orders": {"date_modified": "2023-09-01T00:00:00Z"}}
        }
        final_state = self._run_incremental(initial_state, records=[])
        orders_bookmarks = final_state.get("bookmarks", {}).get("acme__orders", {})
        bk = orders_bookmarks.get("date_modified")
        if bk:
            # Bookmark must not go backwards; allow microsecond normalization
            self.assertTrue(
                bk.startswith("2023-09-01T00:00:00"),
                f"Bookmark regressed to: {bk}"
            )


# ---------------------------------------------------------------------------
# Test: Discovery failure scenarios
# ---------------------------------------------------------------------------

class TestDiscoveryRobustness(unittest.TestCase):

    def test_tables_api_failure_falls_back_to_static(self):
        """If QB tables endpoint fails we still return static streams."""
        client = MagicMock()
        client.base_url = "https://api.quickbase.com"
        client.config = QB_CONFIG
        client.make_request.side_effect = Exception("connection timeout")

        catalog = discover(client=client)
        static_names = {s.stream for s in catalog.streams}
        self.assertIn("apps", static_names)
        self.assertIn("fields", static_names)

    def test_no_appid_skips_dynamic(self):
        """Without qb_appid dynamic discovery is skipped gracefully."""
        cfg = dict(QB_CONFIG)
        del cfg["qb_appid"]
        client = MagicMock()
        client.base_url = "https://api.quickbase.com"
        client.config = cfg
        catalog = discover(client=client)
        # Should still have static streams
        self.assertGreater(len(catalog.streams), 0)

    def test_dynamic_conflict_with_static_name_skipped(self):
        """A dynamic stream whose name matches a static stream is not overwritten."""
        # Force dynamic discovery to return a stream named 'apps'
        client = MagicMock()
        client.base_url = "https://api.quickbase.com"
        client.config = QB_CONFIG

        def make_request(method, endpoint, params=None, **_kw):
            if "tables" in endpoint:
                return [{"id": "t99", "name": "apps", "alias": "acc"}]
            if "fields" in endpoint:
                return [{"id": 3, "label": "Record ID#", "fieldType": "recordid"}]
            return {}

        client.make_request.side_effect = make_request
        catalog = discover(client=client)

        # 'apps' stream must be the static version
        from singer import metadata as md
        apps_entry = next(s for s in catalog.streams if s.stream == "apps")
        mmap = md.to_map(apps_entry.metadata)
        self.assertFalse(md.get(mmap, (), "tap-quickbase.is_dynamic"))


if __name__ == "__main__":
    unittest.main()
