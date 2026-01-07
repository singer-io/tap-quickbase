from base import QuickbaseBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

KNOWN_MISSING_FIELDS = {
    "apps": {
        "ancestorId",
        "dataClassification",
    },
    "fields": {
        "permissions",
    },
    "events": {
        "type",
        "tableId",
        "isActive",
        "owner",
        "id",
        "name",
    },
    "get_reports": {
        "type",
        "usedCount",
        "properties",
        "usedLast",
        "id",
        "name",
        "description",
        "query",
    },
    "get_fields": {
        "findEnabled",
        "label",
        "bold",
        "fieldType",
        "properties",
        "id",
        "doesDataCopy",
        "noWrap",
        "mode",
        "permissions",
        "fieldHelp",
        "required",
        "audited",
        "unique",
        "appearsByDefault",
    },
    "get_field_usage": {
        "tableRules",
        "notifications",
        "dashboards",
        "roles",
        "exactForms",
        "reports",
        "actions",
        "forms",
        "defaultReports",
        "personalReports",
        "webhooks",
        "appHomePages",
        "reminders",
        "pipelines",
        "id",
        "fields",
        "tableImports",
        "relationships",
    },
}


class QuickbaseAllFields(AllFieldsTest, QuickbaseBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

    @staticmethod
    def name():
        return "tap_tester_quickbase_all_fields_test"

    def streams_to_test(self):
        # Exclude streams with no test data available in the test account
        streams_to_exclude = {
            'get_reports',
            'get_field_usage',
            'get_fields',
            'events',
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    def expected_missing_fields(self, stream=None):
        """Return fields that are in the schema but not returned by the API"""
        if not stream:
            return KNOWN_MISSING_FIELDS
        return KNOWN_MISSING_FIELDS.get(stream, set())

