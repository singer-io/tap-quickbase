"""Abstract stream base classes and shared sync/pagination helpers."""

from abc import ABC, abstractmethod
import json
from typing import Any, Dict, Tuple, List, Iterator
from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
    metadata,
    set_currently_syncing,
    write_state
)

LOGGER = get_logger()
DEFAULT_PAGE_SIZE = 100


class BaseStream(ABC):
    """
    A Base Class providing structure and boilerplate for generic streams
    and required attributes for any kind of stream
    ~~~
    Provides:
     - Basic Attributes (stream_name,replication_method,key_properties)
     - Helper methods for catalog generation
     - `sync` and `get_records` method for performing sync
    """

    url_endpoint = ""
    path = ""
    page_size = 100
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    children = []
    parent = ""
    data_key = ""
    parent_bookmark_key = ""
    http_method = "GET"

    def __init__(self, client=None, catalog=None) -> None:
        self.client = client
        self.catalog = catalog
        self.schema = catalog.schema.to_dict()
        self.metadata = metadata.to_map(catalog.metadata)
        self.child_to_sync = []
        self.params = {}
        self.data_payload = {}

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique identifier for the stream.

        This is allowed to be different from the name of the stream, in
        order to allow for sources that have duplicate stream names.
        """

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def replication_keys(self) -> List:
        """Defines the replication key for incremental sync mode of a
        stream."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, str]:
        """List of key properties for stream."""

    def is_selected(self):
        """Return whether the stream is selected in catalog metadata."""
        return metadata.get(self.metadata, (), "selected")

    @abstractmethod
    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """
        Performs a replication sync for the stream.
        ~~~
        Args:
         - state (dict): represents the state file for the tap.
         - transformer (object): A Object of the singer.transformer class.
         - parent_obj (dict): The parent object for the stream.

        Returns:
         - bool: The return value. True for success, False otherwise.

        Docs:
         - https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md
        """

    def get_records(self) -> Iterator:
        """Interacts with api client interaction and pagination."""
        page_size = self.client.config.get("page_size", DEFAULT_PAGE_SIZE)
        skip = 0
        has_more_pages = True
        max_iterations = 10000  # Safety limit to prevent infinite loops

        while has_more_pages and max_iterations > 0:
            max_iterations -= 1

            response = self.client.make_request(
                self.http_method,
                self.url_endpoint,
                self.params,
                self.headers,
                body=json.dumps(self.data_payload) if self.data_payload else None,
                path=self.path
            )

            # Extract records from response
            raw_records = self._extract_records(response)
            num_records = len(raw_records)

            yield from raw_records

            # Stop if no records returned or fewer records than expected page size
            # Most Quickbase endpoints return all records in single response
            if num_records == 0 or num_records < page_size:
                has_more_pages = False
                continue

            if isinstance(response, dict) and "metadata" in response:
                total_records = response["metadata"].get("totalRecords", 0)
                skip += num_records
                if skip >= total_records:
                    has_more_pages = False
            else:
                has_more_pages = False

        if max_iterations == 0:
            LOGGER.warning(
                "Maximum iteration limit reached for stream %s. "
                "This indicates an infinite loop.",
                self.tap_stream_id
            )

    def _extract_records(self, response) -> List:
        """Extract records from API response."""
        if self.data_key:
            return response.get(self.data_key, [])
        if isinstance(response, list):
            return response
        return [response] if response else []

    def write_schema(self) -> None:
        """
        Write a schema message.
        """
        try:
            write_schema(self.tap_stream_id, self.schema, self.key_properties)
        except OSError as err:
            LOGGER.error("OS Error while writing schema for: %s", self.tap_stream_id)
            raise err

    def update_params(self, **kwargs) -> None:
        """
        Update params for the stream
        """
        self.params.update(kwargs)

    def update_data_payload(self, **kwargs) -> None:
        """
        Update JSON body for the stream
        """
        self.data_payload.update(kwargs)

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:  # pylint: disable=unused-argument
        """
        Modify the record before writing to the stream
        """
        return record

    @staticmethod
    def flatten_field_usage_record(record: Dict) -> Dict:
        """Helper to flatten field and usage objects with field.id as primary key."""
        if not record:
            return record
        field = record.get('field', {})
        usage = record.get('usage', {})
        return {'id': field.get('id'), **usage}

    def sync_child_streams(self, state: Dict, transformer: Transformer, record: Dict) -> None:
        """Write schema and sync child streams."""
        for child in self.child_to_sync:
            if child.is_selected():
                child.write_schema()
            set_currently_syncing(state, child.tap_stream_id)
            write_state(state)
            child.sync(state=state, transformer=transformer, parent_obj=record)

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """
        Get the URL endpoint for the stream, handling path parameters.
        """
        if parent_obj is None:
            return f"{self.client.base_url}/{self.path}"

        formatted_path = self.path
        replacements = {
            '{appId}': str(self.client.config.get('app_id', parent_obj.get('id', ''))),
            '{tableId}': self._get_table_id(parent_obj),
            '{fieldId}': str(parent_obj.get('id', '')),
            '{reportId}': str(parent_obj.get('id', ''))
        }

        for placeholder, value in replacements.items():
            if placeholder in formatted_path:
                formatted_path = formatted_path.replace(placeholder, value)

        return f"{self.client.base_url}/{formatted_path}"

    def _get_table_id(self, parent_obj: Dict) -> str:
        """Extract tableId from parent object, handling nested query structure."""
        if 'query' in parent_obj and 'tableId' in parent_obj.get('query', {}):
            return str(parent_obj['query']['tableId'])
        return str(parent_obj.get('id', ''))


class IncrementalStream(BaseStream):
    """Base Class for Incremental Stream."""


    def get_bookmark(self, state: dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config["start_date"],
        )

    def write_bookmark(
        self, state: dict, stream: str, key: Any = None, value: Any = None
    ) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if not (key or self.replication_keys):
            return state

        current_bookmark = get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config["start_date"],
        )
        value = max(current_bookmark, value)
        return write_bookmark(
            state, stream, key or self.replication_keys[0], value
        )


    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Implementation for `type: Incremental` stream."""
        bookmark_date = self.get_bookmark(state, self.tap_stream_id)
        current_max_bookmark_date = bookmark_date
        self.update_params(updated_since=bookmark_date)
        self.url_endpoint = self.get_url_endpoint(parent_obj)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )

                record_bookmark = transformed_record[self.replication_keys[0]]
                if record_bookmark >= bookmark_date:
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed_record)
                        counter.increment()

                    current_max_bookmark_date = max(
                        current_max_bookmark_date, record_bookmark
                    )

                    self.sync_child_streams(state, transformer, record)

            state = self.write_bookmark(
                state, self.tap_stream_id, value=current_max_bookmark_date
            )
            return counter.value


class FullTableStream(BaseStream):
    """Base Class for Full Table Stream."""

    replication_keys = []

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Abstract implementation for `type: Fulltable` stream."""
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

                self.sync_child_streams(state, transformer, record)

            return counter.value


class ParentBaseStream(IncrementalStream):
    """Base Class for Parent Stream."""

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""

        min_parent_bookmark = (
            super().get_bookmark(state, stream) if self.is_selected() else None
        )
        for child in self.child_to_sync:
            bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
            child_bookmark = super().get_bookmark(
                state, child.tap_stream_id, key=bookmark_key
            )
            min_parent_bookmark = (
                min(min_parent_bookmark, child_bookmark)
                if min_parent_bookmark
                else child_bookmark
            )

        return min_parent_bookmark

    def write_bookmark(
        self, state: Dict, stream: str, key: Any = None, value: Any = None
    ) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if self.is_selected():
            super().write_bookmark(state, stream, value=value)

        for child in self.child_to_sync:
            bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
            super().write_bookmark(
                state, child.tap_stream_id, key=bookmark_key, value=value
            )

        return state


class ChildBaseStream(IncrementalStream):
    """Base Class for Child Stream."""
    def __init__(self, client, catalog):
        super().__init__(client, catalog)
        self.bookmark_value = None  # Initialize to avoid access-before-definition

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> int:
        """Singleton bookmark value for child streams."""
        if not self.bookmark_value:
            self.bookmark_value = super().get_bookmark(state, stream)

        return self.bookmark_value
