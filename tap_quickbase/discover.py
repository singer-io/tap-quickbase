"""Catalog discovery for the Quickbase tap.

Static streams are loaded from local JSON schema files.
Dynamic streams are discovered via the QB REST API when a client is supplied.
Both kinds of streams are returned as a single unified
:class:`~singer.catalog.Catalog`.
"""

from typing import Optional

import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_quickbase.schema import get_schemas

LOGGER = singer.get_logger()


def discover(client: Optional[object] = None) -> Catalog:
    """Run discovery mode and return the combined catalog.

    Args:
        client: An authenticated :class:`~tap_quickbase.client.Client`
                instance.  When provided, QB app table streams are also
                discovered dynamically.  When ``None``, only static streams
                are included (backwards-compatible).

    Returns:
        :class:`~singer.catalog.Catalog` containing all discovered streams.
    """
    schemas, field_metadata = get_schemas(client=client)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        try:
            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error("stream_name: %s", stream_name)
            LOGGER.error("type schema_dict: %s", type(schema_dict))
            raise err

        key_properties = metadata.to_map(mdata).get((), {}).get("table-key-properties")

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                metadata=mdata,
            )
        )

    total = len(catalog.streams)
    n_dynamic = sum(
        1 for s in catalog.streams
        if metadata.to_map(s.metadata).get((), {}).get("tap-quickbase.is_dynamic")
    )
    LOGGER.info(
        "discover: catalog contains %s streams (%s static, %s dynamic)",
        total,
        total - n_dynamic,
        n_dynamic,
    )
    return catalog
