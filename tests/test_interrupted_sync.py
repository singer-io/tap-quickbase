
from base import QuickbaseBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class QuickbaseInterruptedSyncTest(InterruptedSyncTest, QuickbaseBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    @staticmethod
    def name():
        return "tap_tester_quickbase_interrupted_sync_test"

    def streams_to_test(self):
        streams_to_exclude = {
            # Unsupported Full-Table Streams
            'events',
            'roles',
            'table_relationships',
            'table_reports',
            'get_reports',
            'fields',
            'get_fields',
            'fields_usage',
            'get_field_usage'
        }
        return self.expected_stream_names().difference(streams_to_exclude)


    def manipulate_state(self):
        return {
            "currently_syncing": "apps",
            "bookmarks": {
                "apps": {},
                "app_tables": {},
                "tables": {},
            }
        }

