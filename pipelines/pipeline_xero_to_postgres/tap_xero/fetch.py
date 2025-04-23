import singer
from datetime import datetime, timezone
from .utility import (
    get_incremental,
    get_incremental_budget,
    process_sub_streams,
    process_budget_lines,
    parse_xero_date,
    write_record,
    format_date,
    parse_date,
)

logger = singer.get_logger()


def handle_xero(resource, url):
    """Handle sync stream"""
    """Bookmark only updated if there's data retrieved"""
    if not url:
        url = resource  # Default to resource if no custom URL is provided

    async def get(session, schemas, state, mdata):
        logger.info(f"Available schemas: {list(schemas.keys())}")
        logger.info(f"Syncing stream: {resource}")
        schema = schemas[resource]

        # Get last sync timestamp from state
        bookmark = state.get(resource)
        last_sync_time = format_date(datetime.now(timezone.utc))
        last_modified_since = None

        if bookmark:
            last_modified_since = parse_date(
                bookmark, "%Y-%m-%dT%H:%M:%S.%fZ"
            ).strftime("%Y-%m-%dT%H:%M:%S")

        logger.info(f"Last sync: {bookmark}" if bookmark else "Fetching all records.")

        async for records in get_incremental(session, url, last_modified_since):
            for row in records:
                if not isinstance(row, dict):
                    raise ValueError(f"Expected dictionary row, but got: {type(row)}")
                write_record(row, resource, schema, mdata, last_sync_time)
                sub_records = []

                sub_records = await process_sub_streams(resource, schemas, [row], mdata)

                for sub_stream, sub_row in sub_records:
                    if sub_stream not in schemas:
                        logger.warning(f"Skipping unknown sub-stream: {sub_stream}")
                        continue  # Ensure the schema exists

                    write_record(
                        sub_row, sub_stream, schemas[sub_stream], mdata, last_sync_time
                    )
        return [(resource, last_sync_time)]

    return get


def handle_xero_budget(resource, url):
    if not url:
        url = resource  # Default to resource if no custom URL provided

    async def get(session, schemas, state, mdata):
        logger.info(f"Available schemas: {list(schemas.keys())}")
        logger.info(f"Syncing budget stream: {resource}")
        schema = schemas[resource]

        # Get last sync timestamp from state.
        bookmark = state.get(resource)
        last_sync_time = format_date(datetime.now(timezone.utc))
        last_modified_since = bookmark if bookmark else None
        logger.info(f"Last sync: {bookmark}" if bookmark else "Fetching all budgets.")

        # Retrieve detailed budget records using get_incremental_budget (assumed similar to get_incremental)
        async for records in get_incremental_budget(session, url, last_modified_since):
            for row in records:
                # Transform parent's UpdatedDateUTC if present.
                if "UpdatedDateUTC" in row and isinstance(row["UpdatedDateUTC"], str):
                    try:
                        iso_date = parse_xero_date(row["UpdatedDateUTC"])
                        dt_obj = datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%S.%fZ")
                        row["UpdatedDateUTC"] = dt_obj.strftime("%Y-%m-%d %H:%M:%S.%f")[
                            :-3
                        ]
                    except Exception as e:
                        logger.error(
                            f"Error processing UpdatedDateUTC '{row['UpdatedDateUTC']}': {e}"
                        )
                if not isinstance(row, dict):
                    raise ValueError(f"Expected dictionary row, but got: {type(row)}")
                # Write the parent budget record.
                write_record(row, resource, schema, mdata, last_sync_time)

                # Process nested budget lines.
                budget_line_records = await process_budget_lines([row], mdata)
                for sub_stream, sub_row in budget_line_records:
                    # Generate composite key based on parent's key, AccountCode, and Period.
                    account_code = sub_row.get("AccountCode")
                    period = sub_row.get("Period")
                    composite_id = f"{sub_row.get('parent_id')}_{account_code}_{period}"
                    sub_row["ID"] = composite_id  # Store composite key as "ID"

                    if sub_stream not in schemas:
                        logger.warning(f"Skipping unknown sub-stream: {sub_stream}")
                        continue  # Ensure the schema exists

                    # Write the flattened budget line record without further date transformation.
                    write_record(
                        sub_row, sub_stream, schemas[sub_stream], mdata, last_sync_time
                    )
        return [(resource, last_sync_time)]

    return get
