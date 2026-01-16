from base import QuickbaseBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest


class QuickbaseAllFields(AllFieldsTest, QuickbaseBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

    MISSING_FIELDS = {
        'apps': {'ancestorId', 'dataClassification'},
        'fields': {'permissions'},
    }

    @staticmethod
    def name():
        return "tap_tester_quickbase_all_fields_test"

    def streams_to_test(self):
        # Exclude child streams with no test data or that cause test hangs
        streams_to_exclude = {
            'events',
            'reports',
        }
        return self.expected_stream_names().difference(streams_to_exclude)
