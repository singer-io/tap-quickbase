# tap-quickbase

This is a [Singer](https://singer.io) tap that produces JSON-formatted data 
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from Quickbase's [API](http://help.quickbase.com/api-guide/index.html)
- Extracts data based on table/column specifications in properties.json
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

    ```bash
    mkvirtualenv -p python3 tap-quickbase
    pip install git+https://github.com/flash716/tap-quickbase.git
    tap-quickbase --config config.json --discover
    tap-quickbase --config config.json --properties properties.json --state state.json
    ```

## Usage

**Install**

```bash
$ mkvirtualenv -p python3 tap-quickbase
$ pip install tap-quickbase
```
or
```bash
$ git clone git@github.com:flash716/tap-quickbase.git
$ cd tap-quickbase
$ mkvirtualenv -p python3 tap-quickbase
$ cd tap-quickbase
$ pip install .
```


**Find your Quickbase Authentication Information**

- Quickbase URL
- AppID
- [User Token](http://help.quickbase.com/api-guide/index.html#create_user_tokens.html)


**Create a configuration file**

Create a JSON file called `config.tap.json` containing the information you just 
found as well as a default start_date to begin pulling data from.

```json
{
 "qb_url": "https://yoursubdomain.quickbase.com/db/",
 "qb_appid": "your_appid",
 "qb_user_token": "your_user_token",
 "start_date": "1970-01-01T00:00:01Z"
}
```


**Discovery mode**

The tap can be invoked in discovery mode to find the available tables and columns 
in the app's data.

```bash
$ tap-quickbase --config config.tap.json --discover > properties.json
```

A discovered catalog is output via stdout to `properties.json`, with a JSON-schema 
description of each table. A source table directly corresponds to a Singer stream.

```json
{
  "streams": [
    {
      "type": "object",
      "key_properties": [
        "rid"
      ],
      "stream_alias": "table_name",
      "table_name": "table_id",
      "tap_stream_id": "app_name__table_name",
      "stream": "app_name__table_name",
      "schema": {
        "properties": {
          "rid": {
            "type": [
              "null",
              "string"
            ],
            "inclusion": "automatic"
          },
          "datecreated": {
            "type": [
              "null",
              "string"
            ],
            "format": "date-time",
            "inclusion": "automatic"
          },
          "datemodified": {
            "type": [
              "null",
              "string"
            ],
            "format": "date-time",
            "inclusion": "automatic"
          },
          "companyid": {
            "type": [
              "null",
              "string"
            ],
            "inclusion": "available"
          }
        }
      },
      "metadata": [
        {
          "metadata": {
            "tap-quickbase.app_id": "app_id"
          },
          "breadcrumb": []
        },
        {
          "metadata": {
            "tap-quickbase.id": "1"
          },
          "breadcrumb": [
            "properties",
            "datecreated"
          ]
        },
        {
          "metadata": {
            "tap-quickbase.id": "2"
          },
          "breadcrumb": [
            "properties",
            "datemodified"
          ]
        },
        {
          "metadata": {
            "tap-quickbase.id": "6"
          },
          "breadcrumb": [
            "properties",
            "companyid"
          ]
        }
      ]
    } 
  ]
}
```

**Field selection**

In sync mode, `tap-quickbase` consumes a modified version of the catalog where 
tables and fields have been marked as _selected_.

Redirect output from the tap's discovery mode to a file so that it can be
modified:

```bash
$ tap-quickbase -c config.tap.json --discover > properties.json
```

Then edit `properties.json` to make selections. 
In this example we want the `table_name` table. 
The stream's schema gets a top-level `selected` flag, as does its columns' schemas:

```json
{
  "streams": [
    {
      "type": "object",
      "selected": "true",
      "key_properties": [
        "rid"
      ],
      "stream_alias": "table_name",
      "table_name": "table_id",
      "tap_stream_id": "app_name__table_name",
      "stream": "app_name__table_name",
      "schema": {
        "properties": {
          "rid": {
            "selected": "true",
            "type": [
              "null",
              "string"
            ],
            "inclusion": "automatic"
          },
          "datecreated": {
            "selected": "true",
            "type": [
              "null",
              "string"
            ],
            "format": "date-time",
            "inclusion": "automatic"
          },
          "datemodified": {
            "selected": "true",
            "type": [
              "null",
              "string"
            ],
            "format": "date-time",
            "inclusion": "automatic"
          },
          "companyid": {
            "selected": "true",
            "type": [
              "null",
              "string"
            ],
            "inclusion": "available"
          }
        }
      },
      "metadata": [
        {
          "metadata": {
            "tap-quickbase.id": "1"
          },
          "breadcrumb": [
            "properties",
            "datecreated"
          ]
        },
        {
          "metadata": {
            "tap-quickbase.id": "2"
          },
          "breadcrumb": [
            "properties",
            "datemodified"
          ]
        },
        {
          "metadata": {
            "tap-quickbase.id": "6"
          },
          "breadcrumb": [
            "properties",
            "companyid"
          ]
        }
      ]
    } 
  ]
}
```

**Sync mode**

With an annotated properties catalog, the tap can be invoked in sync mode:

```bash
$ tap-quickbase -c config.tap.json --properties properties.json
```

Messages are written to standard output following the Singer specification. 
The resultant stream of JSON data can be consumed by a Singer target:

```bash
$ tap-quickbase -c config.tap.json --properties properties.json | target-stitch --config config.target.json
```

## Replication methods and state file

In the above example, we invoked `tap-quickbase` without providing a _state_ file
and without specifying a replication method. The two ways to replicate a given
table are `FULL_TABLE` and `INCREMENTAL`. `FULL_TABLE` replication is used by
default.

### Full Table

Full-table replication extracts all data from the source table each time the tap
is invoked.

### Incremental

Incremental replication works in conjunction with a state file to only extract
new records each time the tap is invoked.


---

Copyright &copy; 2018 Stitch
