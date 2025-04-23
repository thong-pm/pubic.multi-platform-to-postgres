import singer
from datetime import datetime, timezone, timedelta
from .utility import (
    get_incremental,
    process_sub_streams,
    write_record,
    format_date,
    to_wrike_date,
)

logger = singer.get_logger()

SUBSTREAM_MAPPINGS = {
    "contacts": {
        "contacts_profiles": lambda parent_id, sub_record: {
            **sub_record,
            "id": f"{parent_id}_{sub_record.get('accountId', '')}",
        }
    }
}


def handle_wrike(resource, url=None):
    if not url:
        url = resource

    async def get(session, schemas, state, mdata):
        logger.info(f"Available schemas: {list(schemas.keys())}")
        logger.info(f"Syncing stream: {resource}")
        schema = schemas.get(resource, {})

        # Get the last sync (bookmark) and compute the current pipeline run time.
        bookmark = state.get(resource)
        last_sync_time = format_date(datetime.now(timezone.utc))  # ISO8601 string
        wrike_date = to_wrike_date(bookmark) if bookmark else None

        # Delegate incremental filtering to get_incremental, passing only the start value.
        async for row in get_incremental(
            session, url, schemas, extra_query_string={}, start=wrike_date
        ):
            write_record(row, resource, schema, mdata, last_sync_time)

            sub_records = await process_sub_streams(resource, schemas, [row], mdata)
            for sub_stream, sub_row in sub_records:
                # Special processing for substream mappings can be done via a constant mapping.
                mapping = SUBSTREAM_MAPPINGS.get(resource, {}).get(sub_stream)
                if mapping:
                    parent_id = sub_row.get("parent_id", "")
                    sub_row = mapping(parent_id, sub_row)
                if sub_stream not in schemas:
                    logger.warning(f"Skipping unknown sub-stream: {sub_stream}")
                    continue
                write_record(
                    sub_row, sub_stream, schemas[sub_stream], mdata, last_sync_time
                )

        return [
            (resource, last_sync_time)
        ]  # Update state with current pipeline run time

    return get
