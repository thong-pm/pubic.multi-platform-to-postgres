import singer
from datetime import datetime, timezone
from .utility import (
    get_analytics,
    get_incremental,
    get_form_submissions,
    write_record,
    format_date,
    process_sub_streams,
    save_object_config,
    load_object_config,
)

logger = singer.get_logger()
OBJECT_CONFIG = load_object_config()

SUBSTREAM_MAPPINGS = {
    "deals": {
        "deals_companies": lambda parent_id, sub_record: {
            **sub_record,
            "id": f"{parent_id}_{sub_record.get('companies_id', '')}",
        },
        "deals_contacts": lambda parent_id, sub_record: {
            **sub_record,
            "id": f"{parent_id}_{sub_record.get('contacts_id', '')}",
        },
    },
    "contacts": {
        "contacts_companies": lambda parent_id, sub_record: {
            **sub_record,
            "id": f"{parent_id}_{sub_record.get('companies_id', '')}",
        }
    },
    "engagements": {
        "engagements_companies": lambda parent_id, sub_record: {
            **sub_record,
            "id": f"{parent_id}_{sub_record.get('companies_id', '')}",
        },
        "engagements_contacts": lambda parent_id, sub_record: {
            **sub_record,
            "id": f"{parent_id}_{sub_record.get('contacts_id', '')}",
        },
    },
}


def handle_hubspot(resource, url, func=None):
    if not url:
        url = resource

    async def get(session, schemas, state, mdata):
        logger.info(f"Available schemas: {list(schemas.keys())}")
        logger.info(f"Syncing stream: {resource}")
        schema = schemas[resource]

        processed_rows = []
        bookmark = state.get(resource)
        last_sync_time = format_date(
            datetime.now(timezone.utc)
        )  # Set pipeline run time

        if bookmark:
            logger.info(f"Using state (Incremental Sync). Last sync: {bookmark}")
        else:
            logger.info("Fetching all records (full sync).")

        query_string = {}
        async for row in get_incremental(session, resource, url, query_string):
            row_time = row.get("updatedAt")  # Extract modified time

            if resource != "teams":  # Teams do not have updatedAt, process all
                if not row_time:
                    continue  # Skip records without timestamps

                if bookmark and row_time <= bookmark:
                    continue  # Skip records already processed

            if func:
                row = func(row)

            write_record(row, resource, schema, mdata, row_time or last_sync_time)
            processed_rows.append(row)

            sub_records = await process_sub_streams(resource, schemas, [row], mdata)
            for sub_stream, sub_row in sub_records:
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

        return [(resource, last_sync_time)]  # Always update state to pipeline run time

    return get


def handle_hubspot_analytics(resource, url=None, func=None):
    if not url:
        url = resource

    async def get(session, schemas, state, mdata):
        logger.info(f"Available schemas: {list(schemas.keys())}")
        logger.info(f"Syncing stream: {resource}")
        schema = schemas[resource]

        processed_rows = []
        bookmark = state.get(resource)
        last_sync_time = format_date(datetime.now(timezone.utc))
        max_sync_time = bookmark if bookmark else last_sync_time

        if bookmark:
            logger.info(f"Using state (Incremental Sync). Last sync: {bookmark}")
        else:
            logger.info("Fetching all records (full sync).")

        logger.info("Fetching data from HubSpot API...")
        query_string = {}

        async for date_key, records in get_analytics(session, url, query_string):
            if bookmark:
                bookmark_date = bookmark[:10]
                if date_key < bookmark_date:
                    continue

            for row in records:
                breakdown = row.get("breakdown", "unknown")
                row_id = f"{date_key}-{breakdown}"
                row["id"] = row_id
                parsed_date = datetime.strptime(date_key, "%Y-%m-%d")
                row["date_key"] = parsed_date.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

                if func:
                    row = func(row)

                write_record(row, resource, schema, mdata, date_key)
                processed_rows.append(row)

                if date_key > max_sync_time:
                    max_sync_time = date_key

        return [(resource, max_sync_time)]

    return get


def handle_forms(resource, url=None, func=None):
    async def get(session, schemas, state, mdata):
        logger.info(f"Available schemas: {list(schemas.keys())}")
        logger.info(f"Syncing stream: {resource}")
        schema = schemas[resource]
        processed_rows = []
        form_ids = []
        bookmark = state.get(resource)
        last_sync_time = format_date(datetime.now(timezone.utc))

        if bookmark:
            logger.info(f"Using state (Incremental Sync). Last sync: {bookmark}")
        else:
            logger.info("Fetching all records (full sync).")

        query_string = {}

        async for row in get_incremental(session, resource, url, query_string):
            row_time = row.get("updatedAt")
            if not row_time:
                continue

            form_id = row.get("id")
            if form_id:
                form_ids.append(form_id)

            if bookmark and row_time <= bookmark:
                continue

            if func:
                row = func(row)

            write_record(row, resource, schema, mdata, last_sync_time)
            processed_rows.append(row)

        # Save form_ids to object-config immediately
        OBJECT_CONFIG["form_ids"] = form_ids
        save_object_config(OBJECT_CONFIG)
        return [(resource, last_sync_time)]

    return get


def handle_form_submissions(resource):
    async def get(session, schemas, state, mdata):
        logger.info(f"Available schemas: {list(schemas.keys())}")
        logger.info(f"Syncing stream: {resource}")
        schema = schemas[resource]
        processed_rows = []
        form_ids = OBJECT_CONFIG.get("form_ids", [])
        bookmark = state.get(resource)
        last_sync_time = format_date(datetime.now(timezone.utc))
        no_sync_time = "2020-01-01T01:00:0.0Z"

        if not form_ids:
            logger.warning(
                "No form_ids found in OBJECT_CONFIG. Skipping form_submissions."
            )
            return [(resource, no_sync_time)]
        if bookmark:
            logger.info(f"Using state (Incremental Sync). Last sync: {bookmark}")
        else:
            logger.info("Fetching all records (full sync).")

        for form_id in form_ids:
            async for submission in get_form_submissions(session, form_id):
                conversion_id = submission.get("conversionId")
                submitted_at = submission.get("submittedAt")

                if not submitted_at:
                    logger.info("no submit")
                    continue

                if isinstance(submitted_at, int):
                    submitted_at_str = format_date(
                        datetime.utcfromtimestamp(submitted_at / 1000.0)
                    )
                else:
                    submitted_at_str = submitted_at  # assume it's already in ISO format

                submission["submittedAt"] = submitted_at_str
                submission["formId"] = form_id

                # Skip if already synced
                if bookmark and submitted_at_str <= bookmark:
                    continue

                submission["form_id"] = form_id
                if conversion_id:
                    submission["id"] = f"{form_id}_{conversion_id}"

                write_record(submission, resource, schema, mdata, last_sync_time)
                processed_rows.append(submission)

        return [(resource, last_sync_time)]

    return get
