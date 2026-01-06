from base import QuickbaseBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

KNOWN_MISSING_FIELDS = {

}


class QuickbaseAllFields(AllFieldsTest, QuickbaseBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

    @staticmethod
    def name():
        return "tap_tester_quickbase_all_fields_test"

    def streams_to_test(self):
        # Exclude streams with no test data available in the test account
        streams_to_exclude = set()
        return self.expected_stream_names().difference(streams_to_exclude)

