from base import QuickbaseBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class QuickbaseStartDateTest(StartDateTest, QuickbaseBaseTest):
    """Instantiate start date according to the desired data set and run the
    test.
    
    NOTE: All streams use FULL_TABLE replication method and have no
    replication keys. This means:
    - Streams replicate all data on every sync regardless of start_date
    - No incremental replication is available
    
    The tap always fetches the complete dataset for all streams on each sync.
    """

    @staticmethod
    def name():
        return "tap_tester_quickbase_start_date_test"

    def streams_to_test(self):
        # Exclude all streams - none support incremental replication or start_date filtering
        # All Quickbase streams use FULL_TABLE replication with no replication keys
        streams_to_exclude = {
            'apps',
            'events',
            'roles',
            'app_tables',
            'tables',
            'table_relationships',
            'table_reports',
            'get_reports',
            'fields',
            'get_fields',
            'fields_usage',
            'get_field_usage'
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2025-12-01T00:00:00Z"

    @property
    def start_date_2(self):
        return "2026-01-02T00:00:00Z"

    def setUp(self):
        """Skip all tests if no streams support start_date.
        
        All Quickbase streams use FULL_TABLE replication and lack replication keys,
        so start_date filtering is not applicable. The tap always syncs complete
        datasets regardless of the start_date configuration value.
        """
        if not self.streams_to_test():
            self.skipTest("All streams use FULL_TABLE replication - no start_date support")
        super().setUp()
