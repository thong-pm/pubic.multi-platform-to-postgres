# Define url string, handle rate limit, format date, parse date, define writing methods
import time
import asyncio
import singer
from datetime import datetime
import tenacity
import logging
import json

logger = singer.get_logger()

# constants
base_url = "https://www.wrike.com/api/v4/"

page_size = 1000
base_format = "%Y-%m-%dT%H:%M:%S"
unix_format = "%a, %d %b %Y %H:%M:%S %Z"

INCREMENTAL_FIELDS = {
    "audit_log": "eventDate",
    "tasks": "updatedDate",
    "timelogs": "updatedDate",
}

PAGINATE_RESOURCES = {"tasks", "audit_log"}


# Retry logic with tenacity
@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=10),
    before_sleep=tenacity.before_sleep_log(logger, logging.WARNING, exc_info=True),
)
def flatten_json(nested_json, parent_key=""):
    """
    Flatten a nested JSON object.
    For keys that represent substreams (e.g. "profiles"), preserve their original list.
    """
    items = {}
    for k, v in nested_json.items():
        new_key = f"{parent_key}-{k}" if parent_key else k
        # If the key is one we want to treat as a substream, leave it as is.
        if k in ["profiles"]:
            items[k] = v  # Preserve the entire list for substream processing.
        elif isinstance(v, dict):
            items.update(flatten_json(v, new_key))
        elif isinstance(v, list):
            # Flatten list items only if they are not substreams.
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    items.update(flatten_json(item, f"{new_key}-{i}"))
                else:
                    items[f"{new_key}-{i}"] = item
        else:
            items[new_key] = v
    return items


async def get_request(session, resource, qs=None):
    qs = qs or {}
    # If the resource supports incremental sync and a "start" is provided,
    # remove it and build the API filter.
    if resource in INCREMENTAL_FIELDS and "start" in qs:
        start_val = qs.pop("start")
        field_name = INCREMENTAL_FIELDS[resource]
        # Build compact JSON (no extra spaces) for the filter.
        qs[field_name] = json.dumps({"start": start_val}, separators=(",", ":"))

    url = f"{base_url}{resource}{build_query_string(qs)}"

    logger.info(f"Requesting URL: {url}")
    async with await session.get(url, raise_for_status=True) as resp:
        response_data = await resp.json()

    return {
        "data": [flatten_json(row) for row in response_data.get("data", [])],
        "nextPageToken": response_data.get("nextPageToken"),
        "responseSize": response_data.get("responseSize"),
    }


async def get_incremental(
    session, resource, schemas, extra_query_string=None, start=None
):
    """
    Fetch data from Wrike incrementally.

    extra_query_string is passed through directly.
    If a start value is provided, add it to extra_query_string under the key "start".

    Pagination is then handled using nextPageToken.
    """
    extra_query_string = extra_query_string or {}

    # If the resource is neither incremental nor paginated, call API with no params
    if resource not in INCREMENTAL_FIELDS and resource not in PAGINATE_RESOURCES:
        logger.info(f"Fetching {resource} without any params (qs=None)")
        response = await get_request(session, resource, qs=None)
        for row in response["data"]:
            yield row
        return

    if start:
        extra_query_string["start"] = start  # This will be processed in get_request.

    base_params = {**extra_query_string}

    if resource in PAGINATE_RESOURCES:
        # Assign pageSize and handle pagination
        extra_params = {**base_params, "pageSize": page_size}
        next_page_token = None
        response_size = None

        while True:
            params = {**extra_params}
            if next_page_token:
                params["nextPageToken"] = next_page_token

            logger.info(f"Fetching {resource} with params: {params}")
            response = await get_request(session, resource, qs=params)

            for row in response["data"]:
                yield row

            next_page_token = response.get("nextPageToken")
            response_size = response.get("responseSize")
            if not next_page_token or response_size == 0:
                logger.info(
                    f"Checkout {resource}: nextPageToken= {next_page_token} AND responseSize= {response_size}."
                )
                break
    else:
        # Ensure non-paginated incremental resources still get their incremental field
        logger.info(f"Fetching {resource} with incremental params: {base_params}")
        response = await get_request(session, resource, qs=base_params)
        data = response.get("records", [])
        if not data:
            logger.info(
                f"Checkout {resource}: no data found with params: {base_params}."
            )

        for row in response["data"]:
            yield row


async def process_sub_streams(resource, schemas, records, mdata):
    def get_parent_key(mdata_list):
        """
        Determine the parent's key field from mdata.
        This assumes the metadata entry with no breadcrumb contains a "table-key-properties" list.
        """
        for item in mdata_list:
            if not item.get("breadcrumb"):
                # Use the first table-key-property, defaulting to "id"
                return item.get("metadata", {}).get("table-key-properties", ["id"])[0]
        return "id"

    parent_id_field = get_parent_key(mdata)

    processed_sub_records = []

    for record in records:
        parent_id = record.get(parent_id_field)
        if not parent_id:
            logger.warning(f"Skipping record without {parent_id_field}: {record}")
            continue

        # Iterate over each key in the record
        for key, value in record.items():
            if isinstance(value, list) and value:
                # Create a candidate substream name based on the resource and the key
                candidate = f"{resource}_{key.lower()}"
                # Optionally, add custom mappings here if needed (e.g. if key.lower() == "profiles": candidate = f"{resource}_profiles")

                if candidate in schemas:
                    logger.info(
                        f"Processing {len(value)} records for sub-stream '{candidate}' for parent {parent_id}"
                    )
                    for sub_record in value:
                        if isinstance(sub_record, dict):
                            # Attach the parent id to the sub-record
                            sub_record["parent_id"] = parent_id
                            processed_sub_records.append((candidate, sub_record))
                            logger.debug(
                                f"Extracted record for '{candidate}': {sub_record}"
                            )
                        else:
                            logger.warning(
                                f"Expected dict for sub-record in '{candidate}', but got {type(sub_record)}"
                            )
                else:
                    logger.debug(
                        f"Key '{key}' (candidate: '{candidate}') not recognized as sub-stream."
                    )
    return processed_sub_records


def to_wrike_date(iso_str):
    """
    Convert an ISO8601 string (with milliseconds) to the format Wrike requires (without milliseconds).
    For example, "2025-03-29T03:25:58.025Z" -> "2025-03-29T03:25:58Z"
    """
    # Parse using the format that includes milliseconds.
    dt = datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    # Format without milliseconds.
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# TODO: add a link to the relevant documentation for this API or remove the class if there's no rate limiting
# Adapted slightly from https://quentin.pradet.me/blog/how-do-you-rate-limit-calls-with-aiohttp.html
class RateLimiter:
    rate = 1.2  # requests per second

    def __init__(self, client):
        self.client = client
        self.tokens = self.rate
        self.updated_at = time.monotonic()

    async def get(self, *args, **kwargs):
        await self.wait_for_token()
        return self.client.get(*args, **kwargs)

    async def post(self, *args, **kwargs):
        await self.wait_for_token()
        return await self.client.post(*args, **kwargs)

    async def wait_for_token(self):
        while self.tokens < 1:
            self.add_new_tokens()
            await asyncio.sleep(0.1)
        self.tokens -= 1

    # would be nice to just make this an async loop but you can't do that easily in Python, unlike Node
    def add_new_tokens(self):
        now = time.monotonic()
        time_since_update = now - self.updated_at
        new_tokens = time_since_update * self.rate
        self.tokens += new_tokens
        self.updated_at = now


def format_date(dt, format="%Y-%m-%dT%H:%M:%S.%fZ"):
    if isinstance(dt, str):
        return dt  # Already formatted, return as is
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4] + "Z"  # Keep only 3 decimal places


def iso_to_unix(iso_str):
    dt = datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%S")  # Parse ISO string
    unix_time = int(dt.timestamp() * 1000)  # Convert to Unix timestamp (milliseconds)
    return unix_time


def parse_date(str, format=base_format):
    try:
        return datetime.strptime(str, format)
    except Exception:
        if format == base_format:
            raise
        else:
            return datetime.strptime(str, base_format)


def parse_unix_string(str):
    return datetime.strptime(str, unix_format)


def build_query_string(dict):
    if len(dict) == 0:
        return ""

    return "?" + "&".join(["{}={}".format(k, v) for k, v in dict.items()])


def write_record(row, resource, schema, mdata, dt):
    # Singer 6.0.1 breaks the transform step with an import error, so I've done a basic transform step below it (probably much worse but close enough for now).
    # Singer 6.1.0 fixes this but causes a version conflict when trying to install (clashes with Meltano's SDK which target-mssql is using)

    # with singer.Transformer() as transformer:
    #     rec = transformer.transform(row, schema, metadata=metadata.to_map(mdata))
    # singer.write_record(resource, rec, time_extracted=dt)

    rec = {k: v for (k, v) in row.items() if k in schema["properties"]}
    singer.write_record(resource, rec)


# More convenient to use but has to all be held in memory, so use write_record instead for resources with many rows
def write_many(rows, resource, schema, mdata, dt):
    # with metrics.record_counter(resource) as counter:
    for row in rows:
        write_record(row, resource, schema, mdata, dt)
        # counter.increment()
