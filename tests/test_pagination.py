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
        # Exclude streams with insufficient records to test pagination or that cause test hangs
        streams_to_exclude = {
            'apps',
            'roles',
            'app_tables',
            'tables',
            'table_relationships',
            'events',
            'reports',
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    def get_properties(self, original: bool = True):
        """Configuration with reduced page_size to test pagination logic."""
        return {
            "start_date": self.start_date,
            "page_size": 10
        }

    def expected_page_size(self, stream):
        """
        Return the expected page size for pagination testing.

        Overrides the default API_LIMIT to use the configured page_size.
        This allows pagination testing with smaller datasets by setting
        a lower page limit than the API default (100).
        """
        return 10
