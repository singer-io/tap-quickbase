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
    metadata
)

LOGGER = get_logger()


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
    # QuickBase uses skip for pagination, not next
    skip_key = "skip"
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    children = []
    parent = ""
    data_key = ""  # Key to extract records from response
    parent_bookmark_key = ""
    http_method = "GET"  # Default: Most QuickBase endpoints are GET

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
        skip = 0
        has_more = True
        
        while has_more:
            # For single resource endpoints (page_size=None), don't add pagination params
            if self.page_size is not None:
                if self.http_method == "POST":
                    self.data_payload["skip"] = skip
                    self.data_payload["top"] = self.page_size
                else:
                    self.params["skip"] = skip
                    self.params["top"] = self.page_size
                    
            response = self.client.make_request(
                self.http_method,
                self.url_endpoint,
                self.params,
                self.headers,
                body=json.dumps(self.data_payload) if self.data_payload else None,
                path=self.path
            )
            
            # Extract records based on data_key or directly from response
            if self.data_key:
                raw_records = response.get(self.data_key, [])
            elif isinstance(response, list):
                raw_records = response
            else:
                # If no data_key and response is dict, it might be a single object
                raw_records = [response] if response else []
            
            # Check pagination metadata
            metadata = response.get("metadata", {}) if isinstance(response, dict) else {}
            total_records = metadata.get("totalRecords", 0)
            num_records = metadata.get("numRecords", len(raw_records))
            
            # Yield records
            yield from raw_records
            
            # For single resource endpoints, only fetch once
            if self.page_size is None:
                has_more = False
            else:
                # Update skip and check if more records exist
                skip += num_records
                
                # Continue if we got records and haven't reached total
                if isinstance(response, list):
                    # For list responses without metadata, stop if we got fewer than requested
                    has_more = len(raw_records) >= self.page_size
                else:
                    has_more = skip < total_records and num_records > 0

    def write_schema(self) -> None:
        """
        Write a schema message.
        """
        try:
            write_schema(self.tap_stream_id, self.schema, self.key_properties)
        except OSError as err:
            LOGGER.error(
                "OS Error while writing schema for: {}".format(self.tap_stream_id)
            )
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

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """
        Modify the record before writing to the stream
        """
        return record

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """
        Get the URL endpoint for the stream
        """
        return self.url_endpoint or f"{self.client.base_url}/{self.path}"


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

    def write_bookmark(self, state: dict, stream: str, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if not (key or self.replication_keys):
            return state

        current_bookmark = get_bookmark(state, stream, key or self.replication_keys[0], self.client.config["start_date"])
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

                    for child in self.child_to_sync:
                        child.sync(state=state, transformer=transformer, parent_obj=record)

            state = self.write_bookmark(state, self.tap_stream_id, value=current_max_bookmark_date)
            return counter.value


class FullTableStream(BaseStream):
    """Base Class for Full Table Stream."""

    replication_keys = []
    
    def get_url_endpoint(self, parent_obj=None):
        """Prepare URL endpoint for streams, with support for parent object formatting."""
        if parent_obj is None:
            return f"{self.client.base_url}/{self.path}"
        
        # Format path with parent object data
        formatted_path = self.path
        
        # Handle appId - always use from config if available
        if '{appId}' in formatted_path:
            app_id = str(self.client.config.get('appId', parent_obj.get('id', '')))
            formatted_path = formatted_path.replace('{appId}', app_id)
            
        # Handle tableId from parent
        if '{tableId}' in formatted_path:
            # For reports, tableId is nested in query object
            if 'query' in parent_obj and 'tableId' in parent_obj.get('query', {}):
                table_id = str(parent_obj['query']['tableId'])
            else:
                table_id = str(parent_obj.get('id', ''))
            formatted_path = formatted_path.replace('{tableId}', table_id)
            
        # Handle fieldId from parent
        if '{fieldId}' in formatted_path:
            formatted_path = formatted_path.replace('{fieldId}', str(parent_obj.get('id', '')))
            
        # Handle reportId from parent
        if '{reportId}' in formatted_path:
            formatted_path = formatted_path.replace('{reportId}', str(parent_obj.get('id', '')))
            
        return f"{self.client.base_url}/{formatted_path}"

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
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

                for child in self.child_to_sync:
                    child.sync(state=state, transformer=transformer, parent_obj=record)

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

    def get_url_endpoint(self, parent_obj=None):
        """Prepare URL endpoint for child streams."""
        if parent_obj is None:
            return f"{self.client.base_url}/{self.path}"
        
        # Format path with parent object data
        formatted_path = self.path
        
        # Handle appId - always use from config if available
        if '{appId}' in formatted_path:
            app_id = str(self.client.config.get('appId', parent_obj.get('id', '')))
            formatted_path = formatted_path.replace('{appId}', app_id)
            
        # Handle tableId from parent
        if '{tableId}' in formatted_path:
            # For reports, tableId is nested in query object
            if 'query' in parent_obj and 'tableId' in parent_obj.get('query', {}):
                table_id = str(parent_obj['query']['tableId'])
            else:
                table_id = str(parent_obj.get('id', ''))
            formatted_path = formatted_path.replace('{tableId}', table_id)
            
        # Handle fieldId from parent
        if '{fieldId}' in formatted_path:
            formatted_path = formatted_path.replace('{fieldId}', str(parent_obj.get('id', '')))
            
        # Handle reportId from parent
        if '{reportId}' in formatted_path:
            formatted_path = formatted_path.replace('{reportId}', str(parent_obj.get('id', '')))
            
        return f"{self.client.base_url}/{formatted_path}"

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> int:
        """Singleton bookmark value for child streams."""
        if not self.bookmark_value:
            self.bookmark_value = super().get_bookmark(state, stream)

        return self.bookmark_value

