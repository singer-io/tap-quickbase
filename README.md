# tap-quickbase

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This tap:

- Pulls raw data from the [Quickbase API].
- Extracts the following resources:
    - [Apps](https://developer.quickbase.com/operation/getApp)

    - [Events](https://developer.quickbase.com/operation/getAppEvents)

    - [Roles](https://developer.quickbase.com/operation/getRoles)

    - [AppTables](https://developer.quickbase.com/operation/getAppTables)

    - [Tables](https://developer.quickbase.com/operation/getTable)

    - [TableRelationships](https://developer.quickbase.com/operation/getRelationships)

    - [TableReports](https://developer.quickbase.com/operation/getTableReports)

    - [Reports](https://developer.quickbase.com/operation/getReport)

    - [Fields](https://developer.quickbase.com/operation/getFields)

    - [FieldsUsage](https://developer.quickbase.com/operation/getFieldsUsage)

- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Streams


**[apps](https://developer.quickbase.com/operation/getApp)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[events](https://developer.quickbase.com/operation/getAppEvents)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[roles](https://developer.quickbase.com/operation/getRoles)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[app_tables](https://developer.quickbase.com/operation/getAppTables)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[tables](https://developer.quickbase.com/operation/getTable)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[table_relationships](https://developer.quickbase.com/operation/getRelationships)**
- Primary keys: ['id', 'tableId']
- Replication strategy: FULL_TABLE

**[table_reports](https://developer.quickbase.com/operation/getTableReports)**
- Primary keys: ['id', 'tableId']
- Replication strategy: FULL_TABLE

**[reports](https://developer.quickbase.com/operation/getReport)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[fields](https://developer.quickbase.com/operation/getFields)**
- Primary keys: ['id', 'tableId']
- Replication strategy: FULL_TABLE

**[fields_usage](https://developer.quickbase.com/operation/getFieldsUsage)**
- Primary keys: ['id', 'tableId']
- Replication strategy: FULL_TABLE



## Authentication

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-quickbase
    > pip install -e .
    ```
2. Dependent libraries. The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install target-stitch
    > pip install target-json

    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file.  The tap config file for this tap should include these entries:
   - `access_token` (string, required): Quickbase API authentication token
   - `realm_hostname` (string, required): Quickbase realm hostname (e.g., `your-realm.quickbase.com`)
   - `app_id` (string, required): Quickbase application ID
   - `start_date` (string, required): The default value to use if no bookmark exists for an endpoint (rfc3339 date string)
   - `page_size` (integer, optional): Number of records to fetch per page. Default is 100.
   - `request_timeout` (integer, optional): Max time for which request should wait to get a response. Default is 300 seconds.

    ```json
    {
        "access_token": "your_quickbase_access_token",
        "realm_hostname": "your-realm.quickbase.com",
        "app_id": "your_app_id",
        "start_date": "2023-01-01T00:00:00Z",
        "page_size": 100,
        "request_timeout": 300
    }
    ```

    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "engage",
        "bookmarks": {
            "export": "2019-09-27T22:34:39.000000Z",
            "funnels": "2019-09-28T15:30:26.000000Z",
            "revenue": "2019-09-28T18:23:53Z"
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-quickbase --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-quickbase --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-quickbase --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-quickbase --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    While developing the quickbase tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_quickbase -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.67/10
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap_quickbase --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

    #### Unit Tests

    Unit tests may be run with the following.

    ```
    python -m pytest --verbose
    ```

    Note, you may need to install test dependencies.

    ```
    pip install -e .'[dev]'
    ```
---

Copyright &copy; 2019 Stitch
