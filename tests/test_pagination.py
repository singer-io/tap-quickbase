from tap_tester.base_suite_tests.pagination_test import PaginationTest

from base import QuickbaseBaseTest


class QuickbasePaginationTest(PaginationTest, QuickbaseBaseTest):
    """
    Ensure tap can replicate multiple pages of data for streams that use pagination.
    """

    @staticmethod
    def name():
        return "tap_tester_quickbase_pagination_test"

    def streams_to_test(self):
        # Exclude streams with no test data available in the test account
        streams_to_exclude = {
            'get_reports',
            'get_field_usage',
            'get_fields',
            'events',
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    def get_properties(self, original: bool = True):
        """Configuration with reduced page_size to test pagination logic."""
        return {
            "start_date": self.start_date,
            "page_size": 5
        }
