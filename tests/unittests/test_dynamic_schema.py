"""Unit tests for dynamic schema generation (tap_quickbase.dynamic_schema)."""

import unittest
from unittest.mock import MagicMock, patch

from tap_quickbase.dynamic_schema import (
    DATE_MODIFIED_FIELD_ID,
    RECORD_ID_FIELD_ID,
    sanitize_field_name,
    sanitize_stream_name,
    field_type_to_json_schema,
    build_schema_from_fields,
    build_metadata_for_dynamic_stream,
    discover_dynamic_streams,
)
from singer import metadata as singer_metadata


# ---------------------------------------------------------------------------
# sanitize helpers
# ---------------------------------------------------------------------------

class TestSanitizeFieldName(unittest.TestCase):

    def test_lowercase_conversion(self):
        self.assertEqual(sanitize_field_name("MyField"), "myfield")

    def test_spaces_become_underscores(self):
        self.assertEqual(sanitize_field_name("Date Modified"), "date_modified")

    def test_hyphens_become_underscores(self):
        self.assertEqual(sanitize_field_name("first-name"), "first_name")

    def test_non_alphanumeric_stripped(self):
        # Parentheses and % are removed; trailing underscores are also stripped
        result = sanitize_field_name("Field (%)")
        self.assertRegex(result, r"^[a-z0-9_]+$")
        self.assertTrue(result.startswith("field"))

    def test_consecutive_underscores_collapsed(self):
        self.assertEqual(sanitize_field_name("foo  bar"), "foo_bar")

    def test_empty_string_returns_unknown(self):
        self.assertEqual(sanitize_field_name(""), "unknown")

    def test_leading_trailing_underscores_stripped(self):
        self.assertEqual(sanitize_field_name("_foo_"), "foo")


class TestSanitizeStreamName(unittest.TestCase):

    def test_basic_format(self):
        self.assertEqual(sanitize_stream_name("MyApp", "MyTable"), "myapp__mytable")

    def test_special_chars_replaced(self):
        result = sanitize_stream_name("My App!", "My-Table")
        self.assertRegex(result, r"^[a-z0-9_]+$")


# ---------------------------------------------------------------------------
# field_type_to_json_schema
# ---------------------------------------------------------------------------

class TestFieldTypeToJsonSchema(unittest.TestCase):

    def test_checkbox_is_boolean(self):
        schema = field_type_to_json_schema("checkbox")
        self.assertIn("boolean", schema["type"])

    def test_numeric_is_number(self):
        schema = field_type_to_json_schema("numeric")
        self.assertIn("number", schema["type"])

    def test_timestamp_is_string_with_format(self):
        schema = field_type_to_json_schema("timestamp")
        self.assertIn("string", schema["type"])
        self.assertEqual(schema.get("format"), "date-time")

    def test_date_is_string_with_format(self):
        schema = field_type_to_json_schema("date")
        self.assertEqual(schema.get("format"), "date-time")

    def test_text_is_string(self):
        schema = field_type_to_json_schema("text")
        self.assertIn("string", schema["type"])
        self.assertNotIn("format", schema)

    def test_unknown_type_defaults_to_string(self):
        schema = field_type_to_json_schema("totally_new_type")
        self.assertIn("string", schema["type"])

    def test_null_always_included(self):
        for ft in ("checkbox", "numeric", "timestamp", "text"):
            schema = field_type_to_json_schema(ft)
            self.assertIn("null", schema["type"], f"null missing for fieldType={ft}")

    def test_recordid_is_integer(self):
        schema = field_type_to_json_schema("recordid")
        self.assertIn("integer", schema["type"])


# ---------------------------------------------------------------------------
# build_schema_from_fields
# ---------------------------------------------------------------------------

SAMPLE_FIELDS = [
    {"id": 2, "label": "Date Modified", "fieldType": "timestamp"},
    {"id": 3, "label": "Record ID#",    "fieldType": "recordid"},
    {"id": 6, "label": "Name",          "fieldType": "text"},
    {"id": 7, "label": "Active",        "fieldType": "checkbox"},
    {"id": 8, "label": "Score",         "fieldType": "numeric"},
]


class TestBuildSchemaFromFields(unittest.TestCase):

    def setUp(self):
        self.schema, self.fid_map, self.repl_key, self.key_props = (
            build_schema_from_fields(SAMPLE_FIELDS)
        )

    def test_schema_type_is_object(self):
        self.assertEqual(self.schema["type"], "object")

    def test_additional_properties_false(self):
        self.assertFalse(self.schema.get("additionalProperties"))

    def test_all_fields_in_properties(self):
        props = self.schema["properties"]
        self.assertEqual(len(props), len(SAMPLE_FIELDS))

    def test_field_id_to_name_map_populated(self):
        self.assertEqual(len(self.fid_map), len(SAMPLE_FIELDS))
        self.assertIn("2", self.fid_map)
        self.assertIn("3", self.fid_map)

    def test_replication_key_is_date_modified(self):
        date_mod_name = self.fid_map["2"]
        self.assertEqual(self.repl_key, date_mod_name)

    def test_key_property_is_record_id(self):
        record_id_name = self.fid_map["3"]
        self.assertEqual(self.key_props, [record_id_name])

    def test_duplicate_field_names_disambiguated(self):
        dup_fields = [
            {"id": 10, "label": "Status", "fieldType": "text"},
            {"id": 11, "label": "Status", "fieldType": "text"},  # duplicate label
        ]
        schema, fid_map, _, _ = build_schema_from_fields(dup_fields)
        # Both must appear as distinct property names
        self.assertEqual(len(schema["properties"]), 2)
        names = list(schema["properties"].keys())
        self.assertEqual(len(set(names)), 2)

    def test_no_date_modified_field_replication_key_is_none(self):
        fields = [{"id": 6, "label": "Name", "fieldType": "text"}]
        _, _, repl_key, _ = build_schema_from_fields(fields)
        self.assertIsNone(repl_key)

    def test_no_record_id_field_falls_back_to_first(self):
        fields = [{"id": 6, "label": "Name", "fieldType": "text"}]
        _, _, _, key_props = build_schema_from_fields(fields)
        self.assertEqual(len(key_props), 1)

    def test_empty_fields_list(self):
        schema, fid_map, repl_key, key_props = build_schema_from_fields([])
        self.assertEqual(schema["properties"], {})
        self.assertEqual(fid_map, {})
        self.assertIsNone(repl_key)
        self.assertEqual(key_props, [])


# ---------------------------------------------------------------------------
# build_metadata_for_dynamic_stream
# ---------------------------------------------------------------------------

class TestBuildMetadataForDynamicStream(unittest.TestCase):

    def setUp(self):
        self.schema, self.fid_map, self.repl_key, self.key_props = (
            build_schema_from_fields(SAMPLE_FIELDS)
        )
        self.mdata_list = build_metadata_for_dynamic_stream(
            schema=self.schema,
            key_properties=self.key_props,
            replication_key=self.repl_key,
            table_id="bxxxxxxxx",
            field_id_to_name=self.fid_map,
        )

    def _root_meta(self):
        mmap = singer_metadata.to_map(self.mdata_list)
        return singer_metadata.get(mmap, (), {}) or {}

    def test_is_a_list(self):
        self.assertIsInstance(self.mdata_list, list)

    def test_table_id_stored(self):
        mmap = singer_metadata.to_map(self.mdata_list)
        self.assertEqual(singer_metadata.get(mmap, (), "tap-quickbase.table_id"), "bxxxxxxxx")

    def test_field_id_map_stored(self):
        mmap = singer_metadata.to_map(self.mdata_list)
        stored = singer_metadata.get(mmap, (), "tap-quickbase.field_id_map")
        self.assertEqual(stored, self.fid_map)

    def test_is_dynamic_flag_stored(self):
        mmap = singer_metadata.to_map(self.mdata_list)
        self.assertTrue(singer_metadata.get(mmap, (), "tap-quickbase.is_dynamic"))

    def test_key_properties_stored(self):
        mmap = singer_metadata.to_map(self.mdata_list)
        kp = singer_metadata.get(mmap, (), "table-key-properties")
        self.assertEqual(kp, self.key_props)

    def test_replication_key_automatic(self):
        mmap = singer_metadata.to_map(self.mdata_list)
        inclusion = singer_metadata.get(mmap, ("properties", self.repl_key), "inclusion")
        self.assertEqual(inclusion, "automatic")

    def test_full_table_when_no_replication_key(self):
        fields_no_ts = [{"id": 6, "label": "Name", "fieldType": "text"}]
        schema, fid_map, repl_key, key_props = build_schema_from_fields(fields_no_ts)
        mdata = build_metadata_for_dynamic_stream(
            schema=schema,
            key_properties=key_props,
            replication_key=repl_key,  # None
            table_id="t1",
            field_id_to_name=fid_map,
        )
        mmap = singer_metadata.to_map(mdata)
        method = singer_metadata.get(mmap, (), "forced-replication-method")
        self.assertEqual(method, "FULL_TABLE")

    def test_incremental_when_replication_key_present(self):
        mmap = singer_metadata.to_map(self.mdata_list)
        method = singer_metadata.get(mmap, (), "forced-replication-method")
        self.assertEqual(method, "INCREMENTAL")


# ---------------------------------------------------------------------------
# discover_dynamic_streams
# ---------------------------------------------------------------------------

def _make_client(tables=None, fields_by_table=None):
    """Return a mock client with pre-configured API responses."""
    tables = tables or []
    fields_by_table = fields_by_table or {}

    client = MagicMock()
    client.base_url = "https://api.quickbase.com"
    client.config = {"qb_appid": "app123", "start_date": "2023-01-01T00:00:00Z"}

    def make_request_side_effect(method, endpoint, params=None, **_kwargs):
        if "tables" in endpoint:
            return tables
        if "fields" in endpoint:
            tid = (params or {}).get("tableId", "")
            return fields_by_table.get(tid, [])
        return {}

    client.make_request.side_effect = make_request_side_effect
    return client


class TestDiscoverDynamicStreams(unittest.TestCase):

    def test_no_appid_returns_empty(self):
        client = MagicMock()
        client.config = {}
        schemas, mdata = discover_dynamic_streams(client)
        self.assertEqual(schemas, {})
        self.assertEqual(mdata, {})

    def test_api_error_returns_empty(self):
        client = MagicMock()
        client.config = {"qb_appid": "app123"}
        client.base_url = "https://api.quickbase.com"
        client.make_request.side_effect = Exception("network error")
        schemas, mdata = discover_dynamic_streams(client)
        self.assertEqual(schemas, {})

    def test_discovers_tables(self):
        tables = [{"id": "t1", "name": "Orders", "alias": "myapp"}]
        fields = [
            {"id": 2, "label": "Date Modified", "fieldType": "timestamp"},
            {"id": 3, "label": "Record ID#",    "fieldType": "recordid"},
            {"id": 6, "label": "Amount",        "fieldType": "numeric"},
        ]
        client = _make_client(tables=tables, fields_by_table={"t1": fields})
        schemas, mdata = discover_dynamic_streams(client)

        self.assertEqual(len(schemas), 1)
        stream_name = list(schemas.keys())[0]
        self.assertIn("myapp", stream_name)
        self.assertIn("orders", stream_name)

    def test_table_without_id_skipped(self):
        tables = [{"name": "Bad Table", "alias": "myapp"}]  # no 'id'
        client = _make_client(tables=tables)
        schemas, _ = discover_dynamic_streams(client)
        self.assertEqual(schemas, {})

    def test_table_with_empty_fields_skipped(self):
        tables = [{"id": "t1", "name": "Empty", "alias": "myapp"}]
        client = _make_client(tables=tables, fields_by_table={"t1": []})
        schemas, _ = discover_dynamic_streams(client)
        self.assertEqual(schemas, {})

    def test_multiple_tables_discovered(self):
        tables = [
            {"id": "t1", "name": "Orders",   "alias": "myapp"},
            {"id": "t2", "name": "Products", "alias": "myapp"},
        ]
        fields = [
            {"id": 3, "label": "Record ID#", "fieldType": "recordid"},
            {"id": 6, "label": "Name",       "fieldType": "text"},
        ]
        client = _make_client(
            tables=tables,
            fields_by_table={"t1": fields, "t2": fields}
        )
        schemas, mdata = discover_dynamic_streams(client)
        self.assertEqual(len(schemas), 2)

    def test_schema_has_correct_properties(self):
        tables = [{"id": "t1", "name": "Orders", "alias": "myapp"}]
        fields = [
            {"id": 2, "label": "Date Modified", "fieldType": "timestamp"},
            {"id": 3, "label": "Record ID#",    "fieldType": "recordid"},
            {"id": 6, "label": "Amount",        "fieldType": "numeric"},
        ]
        client = _make_client(tables=tables, fields_by_table={"t1": fields})
        schemas, _ = discover_dynamic_streams(client)
        schema = list(schemas.values())[0]
        self.assertIn("properties", schema)
        self.assertEqual(len(schema["properties"]), 3)

    def test_metadata_contains_table_id(self):
        tables = [{"id": "t1", "name": "Orders", "alias": "myapp"}]
        fields = [{"id": 3, "label": "Record ID#", "fieldType": "recordid"}]
        client = _make_client(tables=tables, fields_by_table={"t1": fields})
        _, mdata = discover_dynamic_streams(client)
        stream_mdata = list(mdata.values())[0]
        mmap = singer_metadata.to_map(stream_mdata)
        self.assertEqual(singer_metadata.get(mmap, (), "tap-quickbase.table_id"), "t1")

    def test_fields_api_error_skips_table(self):
        tables = [
            {"id": "t1", "name": "Good",  "alias": "myapp"},
            {"id": "t2", "name": "Error", "alias": "myapp"},
        ]
        good_fields = [{"id": 3, "label": "Record ID#", "fieldType": "recordid"}]

        client = MagicMock()
        client.base_url = "https://api.quickbase.com"
        client.config = {"qb_appid": "app123"}

        def make_request_side_effect(method, endpoint, params=None, **_kwargs):
            if "tables" in endpoint:
                return tables
            tid = (params or {}).get("tableId", "")
            if tid == "t2":
                raise Exception("forbidden")
            return good_fields

        client.make_request.side_effect = make_request_side_effect
        schemas, _ = discover_dynamic_streams(client)
        # Only the good table should be discovered
        self.assertEqual(len(schemas), 1)


if __name__ == "__main__":
    unittest.main()
