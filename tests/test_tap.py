import unittest
import tap_quickbase
import singer.metadata as singer_metadata

from .mock_connection import MockConnection


class TestDiscoverCatalog(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.conn = MockConnection()
        cls.catalog = tap_quickbase.discover_catalog(cls.conn)

    def test_tables_length(self):
        self.assertEqual(1, len(self.catalog.streams))

    def test_tap_stream_id(self):
        self.assertEqual("app_name__table_name", self.catalog.streams[0].tap_stream_id)

    def test_app_metadata(self):
        metadata = singer_metadata.to_map(self.catalog.streams[0].metadata)
        self.assertEqual("app_id", singer_metadata.get(metadata, tuple(), "tap-quickbase.app_id"))

    def test_key_properties(self):
        self.assertEqual(1, len(self.catalog.streams[0].key_properties))
        self.assertEqual("rid", self.catalog.streams[0].key_properties[0])

    def test_discovered_properties(self):
        api_fields = set([f["name"] for f in self.conn.get_fields('1').values()])
        schema_fields = set(self.catalog.streams[0].schema.properties.keys())
        api_fields.remove('child_text_field') # Children are nested
        schema_fields.remove('rid') # rid is added artificially in discovery mode
        self.assertEqual(api_fields, schema_fields)

    def test_properties_rid_automatic(self):
        metadata = singer_metadata.to_map(self.catalog.streams[0].metadata)
        self.assertEqual(
            "automatic",
            singer_metadata.get(metadata, ("properties", "rid"), "inclusion")
        )

    def test_properties_timestamp(self):
        self.assertEqual(
            "string",
            self.catalog.streams[0].schema.properties['datecreated'].type[1]
        )
        self.assertEqual(
            "date-time",
            self.catalog.streams[0].schema.properties['datecreated'].format
        )

    def test_properties_string(self):
        self.assertEqual(
            "string",
            self.catalog.streams[0].schema.properties['text_field'].type[1]
        )

    def test_properties_boolean(self):
        self.assertEqual(
            "boolean",
            self.catalog.streams[0].schema.properties['boolean_field'].type[1]
        )

    def test_properties_float(self):
        self.assertEqual(
            "number",
            self.catalog.streams[0].schema.properties['float_field'].type[1]
        )

    def test_metadata_length(self):
        additional_metadata_count = 2 # app_id root level, and rid record
        self.assertEqual(len(self.conn.get_fields('1')) + additional_metadata_count,
                         len(self.catalog.streams[0].metadata))

    def test_metadata_datecreated_id(self):
        found_breadcrumb = False
        for meta in self.catalog.streams[0].metadata:
            if tuple(meta['breadcrumb']) == ("properties", "datecreated", ):
                found_breadcrumb = True
                self.assertEqual("1", meta['metadata']['tap-quickbase.id'])
        self.assertTrue(found_breadcrumb)

    def test_child_field(self):
        composite_name = tap_quickbase.format_child_field_name("parent_field", "child_text_field")
        pieces = composite_name.split('.')
        parent_name = pieces[0]
        child_name = pieces[1]
        self.assertTrue(
            child_name in self.catalog.streams[0].schema.properties[parent_name].properties
        )


class TestBuildFieldList(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.conn = MockConnection()
        cls.catalog = tap_quickbase.discover_catalog(cls.conn)
        cls.schema = cls.catalog.streams[0].schema
        cls.properties = cls.schema.properties
        cls.metadata = singer_metadata.to_map(cls.catalog.streams[0].metadata)

    def test_build_field_list(self):
        # by default only datemodified is included as a query field
        field_list, ids_to_breadcrumbs = tap_quickbase.build_field_lists(self.schema, self.metadata, [])
        self.assertEqual(1, len(field_list))
        self.assertEqual(['properties', 'datemodified'], ids_to_breadcrumbs['2'])

    def test_build_field_list_include_datecreated(self):
        singer_metadata.write(self.metadata, ('properties','datecreated'), 'selected', True)
        field_list, ids_to_breadcrumbs = tap_quickbase.build_field_lists(self.schema, self.metadata, [])
        self.assertEqual(2, len(field_list))
        self.assertEqual(['properties', 'datecreated'], ids_to_breadcrumbs['1'])
