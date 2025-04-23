import os
import json
import singer
from .vendored import metadata


# import singer.metrics as metrics

import asyncio

from .config import (
    SYNC_FUNCTIONS,
    SUB_STREAMS,
    format_bookmark,
    get_id_column,
)


#####################################################################################################################################
#####################################################################################################################################
#
#
# WARNING
# Only edit this file if you really know what you're doing.
# You normally will never need to edit this file, or even read it.
#
#
#####################################################################################################################################
#####################################################################################################################################

logger = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def populate_metadata(schema_name, schema):
    mdata = metadata.new()
    id_column = get_id_column(schema_name)
    mdata = metadata.write(mdata, (), "table-key-properties", [id_column])

    for field_name in schema["properties"].keys():
        mdata = metadata.write(
            mdata,
            ("properties", field_name),
            "inclusion",
            "automatic" if field_name == id_column else "available",
        )

    return mdata


def get_catalog():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():
        # get metadata for each field
        mdata = populate_metadata(schema_name, schema)

        # create and add catalog entry
        catalog_entry = {
            "stream": schema_name,
            "tap_stream_id": schema_name,
            "schema": schema,
            "metadata": metadata.to_list(mdata),
            "key_properties": [get_id_column(schema_name)],
        }
        streams.append(catalog_entry)

    return {"streams": streams}


def do_discover():
    catalog = get_catalog()
    # dump catalog
    print(json.dumps(catalog, indent=2))


def get_selected_streams(catalog):
    """
    Gets selected streams.  Checks schema's 'selected'
    first -- and then checks metadata, looking for an empty
    breadcrumb and mdata with a 'selected' entry
    """
    selected_streams = []
    for stream in catalog["streams"]:
        stream_metadata = stream["metadata"]
        if stream["schema"].get("selected", False):
            selected_streams.append(stream["tap_stream_id"])
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry["breadcrumb"] and entry["metadata"].get("selected", None):
                    selected_streams.append(stream["tap_stream_id"])

    return selected_streams


def get_stream_from_catalog(stream_id, catalog):
    for stream in catalog["streams"]:
        if stream["tap_stream_id"] == stream_id:
            return stream
    return None


async def do_sync(session, state, catalog):
    selected_stream_ids = get_selected_streams(catalog)
    logger.info(f"Selected streams: {selected_stream_ids}")

    # pipelinewise-target-redshift fails without this initial state message, per https://github.com/transferwise/pipelinewise-target-redshift/issues/69
    singer.write_state(state)

    # sync streams in parallel
    streams = []

    for stream in catalog["streams"]:
        stream_id = stream["tap_stream_id"]
        stream_schema = stream["schema"]
        mdata = stream["metadata"]

        # if it is a substream, it will be synced by its parent
        if not SYNC_FUNCTIONS.get(stream_id):
            continue

        # if stream is selected, write schema and sync
        if stream_id in selected_stream_ids:
            singer.write_schema(stream_id, stream_schema, stream["key_properties"])

            # get sync function and any sub streams
            sync_func = SYNC_FUNCTIONS[stream_id]
            sub_stream_ids = SUB_STREAMS.get(stream_id, [])

            # sync stream
            stream_schemas = {stream_id: stream_schema}

            # get and write selected substream schemas; no-op if no substreams are selected
            for sub_stream_id in sub_stream_ids:
                if sub_stream_id in selected_stream_ids:
                    sub_stream = get_stream_from_catalog(sub_stream_id, catalog)
                    stream_schemas[sub_stream_id] = sub_stream["schema"]
                    singer.write_schema(
                        sub_stream_id,
                        sub_stream["schema"],
                        sub_stream["key_properties"],
                    )

            # sync stream and its sub streams
            streams.append(sync_func(session, stream_schemas, state, mdata))

    streams_resolved = await asyncio.gather(*streams)

    # update bookmark by merging in all streams
    for stream in streams_resolved:
        for resource, extraction_time in stream:
            state[resource] = format_bookmark(stream, extraction_time)
    singer.write_state(state)


def main_impl(required_config_keys, handler):
    args = singer.utils.parse_args(required_config_keys)

    if args.discover:
        do_discover()
    else:  # Sync operation block
        # --- Determine Catalog Source (Keep the corrected logic from previous step) ---
        catalog_object = None  # Initialize
        if args.catalog:
            catalog_object = (
                args.catalog
            )  # This should be the singer.catalog.Catalog object
            logger.info("Using catalog object provided via --catalog argument.")
        elif args.properties:
            catalog_object = args.properties
            logger.info("Using catalog object provided via --properties argument.")

        if catalog_object is None:
            logger.error(
                "CRITICAL: Sync operation requires a catalog file (--catalog). None provided or loaded."
            )
            raise ValueError(
                "Sync operation requires a catalog file provided via --catalog."
            )
        # --- End Determine Catalog Source ---

        # --- NEW: Convert Catalog Object back to Dictionary ---
        catalog_dict = None
        if hasattr(catalog_object, "to_dict"):
            # If the object has a 'to_dict' method, use it
            catalog_dict = catalog_object.to_dict()
            logger.info("Converted Catalog object back to dictionary using .to_dict()")
        elif isinstance(catalog_object, dict):
            # If it was already somehow a dictionary, use it directly
            catalog_dict = catalog_object
            logger.info("Catalog object was already a dictionary.")
        else:
            # If we have an object but cannot convert it, raise an error
            logger.error(
                f"Loaded catalog object (type: {type(catalog_object)}) has no 'to_dict' method and is not a dictionary. Cannot proceed."
            )
            raise TypeError(
                "Cannot convert loaded catalog object to the dictionary format required by downstream functions."
            )

        if catalog_dict is None or "streams" not in catalog_dict:
            logger.error(
                "Conversion to dictionary failed or resulting dictionary is missing 'streams' key."
            )
            raise ValueError("Failed to obtain a valid catalog dictionary structure.")
        # --- End Conversion ---

        # Get state
        starting_state = args.state.get("value", args.state)
        logger.info(f"Starting sync with state: {json.dumps(starting_state)}")

        # Call the handler (do_sync) with the dictionary version of the catalog
        asyncio.run(handler(args.config, starting_state, catalog_dict))
