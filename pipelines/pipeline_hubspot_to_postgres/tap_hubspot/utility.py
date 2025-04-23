# Define url string, handle rate limit, format date, parse date, define writing methods
import time
import asyncio
import singer
from datetime import datetime, timedelta
import tenacity
import logging
import json
import os

logger = singer.get_logger()

# constants
base_url = "https://api.hubapi.com/crm/v3/"
team_url = "https://api.hubapi.com/settings/v3/users/teams"
marketing_url = "https://api.hubapi.com/analytics/v2/reports/sources/daily"
form_url = "https://api.hubapi.com/marketing/v3/forms"
formSubmissions_url = "https://api.hubapi.com/form-integrations/v1/submissions/forms"

page_size = 100
base_format = "%Y-%m-%dT%H:%M:%S"
unix_format = "%a, %d %b %Y %H:%M:%S %Z"
historical_start_date = int(
    (datetime.now() - timedelta(days=2)).timestamp() * 1000
)  # 2 days ago
one_day = 86400000


# Retry logic with tenacity
@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=10),
    before_sleep=tenacity.before_sleep_log(logger, logging.WARNING, exc_info=True),
)
def load_object_config():
    json_file = os.path.join(os.path.dirname(__file__), "object-config.json")
    with open(json_file, "r", encoding="utf-8") as f:
        return json.load(f)


def save_object_config(config):
    json_file = os.path.join(os.path.dirname(__file__), "object-config.json")
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)


# Load config at startup
OBJECT_CONFIG = load_object_config()


async def get_request(session, resource, url, qs=None):
    qs = qs or {}

    if resource == "teams":
        url = team_url
    elif resource == "marketings":
        url = marketing_url
    elif resource == "forms":
        url = form_url
    elif resource == "form_submissions":
        logger.info("Skipping...form_submissions is handled by get_form_submissions()")
        return {}
    else:
        url = f"{base_url}{url}"

    query_string = build_query_string(qs)
    api_url = f"{url}{query_string}" if query_string else url

    logger.info(f"Calling URL: {api_url}")

    async with await session.get(api_url, raise_for_status=True) as resp:
        return await resp.json()


async def get_incremental(session, resource, url, extra_query_string=None):
    if extra_query_string is None:
        extra_query_string = {}

    object_config = OBJECT_CONFIG.get(resource, {})  # Get config from JSON
    associations = ",".join(object_config.get("associations", []))
    properties = ",".join(object_config.get("properties", []))

    query_params = {**extra_query_string, "limit": page_size}

    if associations:
        logger.info(f"Associations: {associations}")
        query_params["associations"] = associations
    if properties:
        query_params["properties"] = properties

    after = None  # Pagination starts without an `after` cursor
    while True:
        if after:
            query_params["after"] = after  # Add pagination cursor

        response = await get_request(session, resource, url, query_params)

        for row in response.get("results", []):
            row.update(row.pop("properties", {}))  # Flatten properties
            yield row  # Process each row one by one

        after = (
            response.get("paging", {}).get("next", {}).get("after")
        )  # Get `after` cursor
        if not after:
            break  # Stop if no more pages


async def get_analytics(session, resource, url, extra_query_string=None):
    extra_query_string = extra_query_string or {}

    # Call your fixed get_request to get the raw JSON response
    response = await get_request(session, resource, url, extra_query_string)

    for date_key, records in response.items():
        yield date_key, records


async def get_form_submissions(session, form_id, extra_query_string=None):
    if not form_id:
        logger.warning("Missing form_id in get_form_submissions(). Skipping.")
        return

    url = f"{formSubmissions_url}/{form_id}"
    query_params = extra_query_string or {}
    query_params["limit"] = 50

    after = None
    while True:
        if after:
            query_params["after"] = after

        query_string = build_query_string(query_params)
        api_url = f"{url}{query_string}" if query_string else url

        logger.info(f"Fetching form submissions from: {api_url}")
        async with await session.get(api_url, raise_for_status=True) as resp:
            data = await resp.json()
            results = data.get("results", [])
            if not results:
                logger.info(f"No submissions for form_id {form_id}")

            for submission in results:
                yield submission

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break


async def process_sub_streams(resource, schemas, records, mdata):
    """
    Process sub-streams dynamically based on object-config.json.
    Extract associations and attach the correct parent_id using get_parent_key().
    Flatten association keys consistently (e.g., companies_id, companies_type).
    """

    def get_parent_key(mdata_list):
        """
        Determine the parent's key field from mdata.
        Assumes the metadata entry with no breadcrumb contains a "table-key-properties" list.
        """
        for item in mdata_list:
            if not item.get("breadcrumb"):
                # Use the first table-key-property, defaulting to "id"
                return item.get("metadata", {}).get("table-key-properties", ["id"])[0]
        return "id"

    parent_id_field = get_parent_key(mdata)  # Dynamically determine parent ID field
    object_config = OBJECT_CONFIG.get(resource, {})  # Get object config
    associations_list = object_config.get(
        "associations", []
    )  # Get associations from config

    processed_sub_records = []

    for record in records:
        parent_id = record.get(parent_id_field)
        if not parent_id:
            logger.warning(f"Skipping record without {parent_id_field}: {record}")
            continue

        # Iterate over configured associations dynamically
        for association_key in associations_list:
            associated_records = (
                record.get("associations", {})
                .get(association_key, {})
                .get("results", [])
            )

            if associated_records:
                candidate = f"{resource}_{association_key}"  # e.g., deals_companies, deals_contacts

                if candidate in schemas:
                    logger.info(
                        f"Processing {len(associated_records)} records for sub-stream '{candidate}' (Parent ID: {parent_id})"
                    )
                    for sub_record in associated_records:
                        if isinstance(sub_record, dict):
                            flattened_sub_record = {}

                            # Iterate over all keys in the sub-record
                            for key, value in sub_record.items():
                                # Join the association name with the key to create the new field name
                                new_key = f"{association_key}_{key}"  # e.g., companies_id, companies_type
                                flattened_sub_record[new_key] = value

                            # Add parent_id and normalize the association structure
                            flattened_sub_record["parent_id"] = parent_id

                            processed_sub_records.append(
                                (candidate, flattened_sub_record)
                            )
                            logger.debug(
                                f"Flattened substream '{candidate}': {flattened_sub_record}"
                            )
                        else:
                            logger.warning(
                                f"Expected dict for sub-record in '{candidate}', but got {type(sub_record)}"
                            )
                else:
                    logger.debug(
                        f"Skipping unknown sub-stream: '{candidate}' (Not in schemas)"
                    )

    return processed_sub_records


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
