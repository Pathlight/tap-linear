#!/usr/bin/env python3
import os
import json
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from .client import GraphQLClient



REQUIRED_CONFIG_KEYS = ["base_url", "api_key"]
LOGGER = singer.get_logger()
JOIN_TABLES = {
    'memberships': {
        'tables': ['teams', 'users'],
        'field': 'memberships'
    },
    'issue_labels': {
        'tables': ['issues', 'labels'],
        'field': 'labels'
    },
    'project_team': {
        'tables': ['projects', 'teams'],
        'field': 'teams'
    }
}


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=['id'],
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    def remove_nodes(row):
        new_row = row
        for key, value in row.items():
            if isinstance(value, dict) and list(value.keys()) == ['nodes']:
                new_row[key] = value['nodes']
        return new_row
    client = GraphQLClient(config)

    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=['id'],
        )
        if stream.tap_stream_id in JOIN_TABLES:
            tap_data = client.get_join_through_data(stream.tap_stream_id, JOIN_TABLES[stream.tap_stream_id])
        else: 
            tap_data = client.get(stream.tap_stream_id, stream.schema.to_dict(), is_singular=(stream.tap_stream_id[-1]!='s'))
        max_bookmark = None
        for row in tap_data:
            row = remove_nodes(row)
            singer.write_records(stream.tap_stream_id, [row])
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
