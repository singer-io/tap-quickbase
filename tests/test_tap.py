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
        self.assertEqual("database_name__table_name", self.catalog.streams[0].tap_stream_id)

    def test_database(self):
        self.assertEqual("database_id", self.catalog.streams[0].database)

    def test_key_properties(self):
        self.assertEqual(1, len(self.catalog.streams[0].key_properties))
        self.assertEqual("rid", self.catalog.streams[0].key_properties[0])

    def test_properties_length(self):
        # RID is added to the schema properties automatically
        self.assertEqual(len(self.conn.get_fields('1')) + 1, len(self.catalog.streams[0].schema.properties.keys()))

    def test_properties_rid_automatic(self):
        self.assertEqual(
            "automatic",
            self.catalog.streams[0].schema.properties['rid'].inclusion
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
        self.assertEqual(len(self.conn.get_fields('1')), len(self.catalog.streams[0].metadata))

    def test_metadata_datecreated_id(self):
        found_breadcrumb = False
        for meta in self.catalog.streams[0].metadata:
            if tuple(meta['breadcrumb']) == ("properties", "datecreated", ):
                found_breadcrumb = True
                self.assertEqual("1", meta['metadata']['tap-quickbase.id'])
        self.assertTrue(found_breadcrumb)

    def test_child_field(self):
        self.assertTrue(
            "text_field - child_text_field" in self.catalog.streams[0].schema.properties
        )


class TestBuildFieldList(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.conn = MockConnection()
        cls.catalog = tap_quickbase.discover_catalog(cls.conn)
        cls.properties = cls.catalog.streams[0].schema.properties
        cls.metadata = singer_metadata.to_map(cls.catalog.streams[0].metadata)

    def test_build_field_list(self):
        # by default only datemodified is included as a query field
        field_list, ids_to_names = tap_quickbase.build_field_lists(self.properties, self.metadata)
        self.assertEqual(1, len(field_list))
        self.assertEqual('datemodified', ids_to_names['2'])

    def test_build_field_list_include_datecreated(self):
        self.properties['datecreated'].selected = 'true'
        field_list, ids_to_names = tap_quickbase.build_field_lists(self.properties, self.metadata)
        self.assertEqual(2, len(field_list))
        self.assertEqual('datecreated', ids_to_names['1'])
