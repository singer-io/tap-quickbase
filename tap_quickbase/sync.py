"""Sync orchestration for selected streams."""

from typing import Dict

import singer
from tap_quickbase.streams import STREAMS
from tap_quickbase.client import Client

LOGGER = singer.get_logger()


def update_currently_syncing(state: Dict, stream_name: str) -> None:
    """
    Update currently_syncing in state and write it
    """
    if not stream_name and singer.get_currently_syncing(state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def write_schema(stream, client, streams_to_sync, catalog) -> None:
    """
    Write schema for stream and its children
    """
    if stream.is_selected():
        stream.write_schema()

    for child in stream.children:
        child_obj = STREAMS[child](client, catalog.get_stream(child))
        write_schema(child_obj, client, streams_to_sync, catalog)
        if child in streams_to_sync:
            stream.child_to_sync.append(child_obj)


def sync(client: Client, config: Dict, catalog: singer.Catalog, state) -> None:  # pylint: disable=unused-argument
    """
    Sync selected streams from catalog
    """

    streams_to_sync = []
    for stream in catalog.get_selected_streams(state):
        streams_to_sync.append(stream.stream)
    LOGGER.info("selected_streams: %s", streams_to_sync)

    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("last/currently syncing stream: %s", last_stream)

    with singer.Transformer() as transformer:
        index = 0
        while index < len(streams_to_sync):
            stream_name = streams_to_sync[index]
            index += 1

            stream = STREAMS[stream_name](client, catalog.get_stream(stream_name))
            if stream.parent:
                if stream.parent not in streams_to_sync:
                    streams_to_sync.append(stream.parent)
                continue

            write_schema(stream, client, streams_to_sync, catalog)
            LOGGER.info("START Syncing: %s", stream_name)
            update_currently_syncing(state, stream_name)
            total_records = stream.sync(state=state, transformer=transformer)

            update_currently_syncing(state, None)
            LOGGER.info(
                "FINISHED Syncing: %s, total_records: %s", stream_name, total_records
            )
