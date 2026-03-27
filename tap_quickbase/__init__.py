"""Singer tap for Quickbase."""

import sys
import json
import singer
from tap_quickbase.client import Client
import tap_quickbase.discover as _discover_mod
import tap_quickbase.sync as _sync_mod

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ['qb_user_token', 'qb_appid', 'qb_url', 'start_date']


def do_discover(client: Client) -> None:
    """Discover and emit the catalog (static + dynamic streams) to stdout."""
    LOGGER.info("Starting discover")
    catalog = _discover_mod.discover(client=client)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info("Finished discover")


@singer.utils.handle_top_exception(LOGGER)
def main():
    """
    Run the tap
    """
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    state = {}
    if parsed_args.state:
        state = parsed_args.state

    with Client(parsed_args.config) as client:
        if parsed_args.discover:
            do_discover(client)
        elif parsed_args.catalog:
            _sync_mod.sync(
                client=client,
                config=parsed_args.config,
                catalog=parsed_args.catalog,
                state=state)


if __name__ == "__main__":
    main()
