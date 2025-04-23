# Define url string, handle rate limit, format date, parse date, define writing methods
import time
import asyncio
import singer
from .vendored import metadata
from datetime import datetime
from dateutil.relativedelta import relativedelta
import tenacity
import logging
import os
import json

logger = singer.get_logger()

# constants
TOKEN_FILE = "token_cache_xero.json"
base_url = "https://api.xero.com/api.xro/2.0/"
base_format = "%Y-%m-%dT%H:%M:%S"
unix_format = "%a, %d %b %Y %H:%M:%S %Z"
one_day = 86400000
INITIAL_START_DATE = "2021-01-01"


# Retry logic with tenacity
@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=10),
    before_sleep=tenacity.before_sleep_log(logger, logging.WARNING, exc_info=True),
)

# Load tokens from cache if they exist.

def load_tokens():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            return json.load(f)
    return {}


async def get_request(session, resource, tenant_id, last_modified_since, query_params):
    query_string = build_query_string(query_params)
    url = f"{base_url}{resource}{query_string}"
    logger.info(f"Fetching URL: {url}")

    tokens = load_tokens()
    if not tokens:
        raise Exception("No tokens found. Ensure authentication is complete.")

    access_token = tokens["access_token"]
    # Get the full tenant info from tokens (assuming it's stored there)
    tenant_info = next((t for t in tokens["tenants"] if t["id"] == tenant_id), {})

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Xero-tenant-id": tenant_id,
    }

    if last_modified_since:
        headers["If-Modified-Since"] = last_modified_since
        logger.info(f"If-Modified-Since: {last_modified_since}")

    results = []
    async with await session.get(url, headers=headers, raise_for_status=True) as resp:
        data = await resp.json()
        pagination = data.get("pagination", {})
        total_pages = pagination.get("pageCount", 1)
        current_page = pagination.get("page", 1)
        records_list = data.get(resource.title(), [])

        # Inject tenant details into each record
        for record in records_list:
            record["tenant_id"] = tenant_id
            record["tenant_name"] = tenant_info.get("name", "")
            results.append(record)

    return results, current_page, total_pages


async def get_incremental(session, resource, last_modified_since):
    tokens = load_tokens()
    tenants = tokens["tenants"]

    for tenant in tenants:
        tenant_id = tenant["id"]
        logger.info(f"Processing tenant: {tenant['name']}")

        records, _, total_pages = await get_request(
            session, resource, tenant_id, last_modified_since, {"page": 1}
        )

        for page in range(2, total_pages + 1):
            query_params = {"page": page}
            records, _, _ = await get_request(
                session, resource, tenant_id, last_modified_since, query_params
            )

            if records:
                yield records
            else:
                yield []


async def process_sub_streams(resource, schemas, records, mdata):
    def get_parent_key(mdata_list):
        for item in mdata_list:
            if not item.get("breadcrumb"):
                # This assumes "table-key-properties" is a list; take the first element.
                return item.get("metadata", {}).get("table-key-properties", ["ID"])[0]
        return "ID"

    parent_id_field = get_parent_key(mdata)

    processed_sub_records = []

    for record in records:
        parent_id = record.get(parent_id_field)
        if not parent_id:
            logger.warning(f"Skipping record without {parent_id_field}: {record}")
            continue  # Skip records without a parent ID

        # Iterate over every key in the record
        for key, value in record.items():
            if isinstance(value, list) and value:
                # Construct a candidate substream name by convention
                candidate = f"{resource}_{key.lower()}"
                # (For example, if resource is "invoices" and key is "LineItems", candidate becomes "invoices_lineitems")
                # If you need to map "LineItems" to "invoices_lines", you can adjust the candidate here:
                if key.lower() == "lineitems":
                    candidate = f"{resource}_lines"

                if candidate in schemas:
                    logger.info(
                        f"Processing {len(value)} records for sub-stream '{candidate}' for parent {parent_id}"
                    )
                    for sub_record in value:
                        if isinstance(sub_record, dict):
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


async def get_request_budget(
    session, resource, tenant_id, last_sync_time, query_params
):
    query_string = build_query_string(query_params)
    url = f"{base_url}/{resource}{query_string}"
    logger.info(f"Fetching URL: {url}")

    tokens = load_tokens()
    if not tokens:
        raise Exception("No tokens found. Ensure authentication is complete.")

    access_token = tokens["access_token"]
    # Get full tenant info from tokens
    tenant_info = next((t for t in tokens["tenants"] if t["id"] == tenant_id), {})

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Xero-tenant-id": tenant_id,
    }

    results = []
    async with await session.get(url, headers=headers, raise_for_status=True) as resp:
        data = await resp.json()
        budgets = data.get("Budgets", [])
        for budget in budgets:
            # Parse the UpdatedDateUTC field
            updated_date_str = budget.get("UpdatedDateUTC", "")
            updated_dt = parse_xero_date(updated_date_str)

            if last_sync_time and updated_dt:
                if last_sync_time and updated_dt <= last_sync_time:
                    continue

            budget["tenant_id"] = tenant_id
            budget["tenant_name"] = tenant_info.get("name", "")
            results.append(budget)
    return results, 1, 1


def compute_final_date():
    today = datetime.now()
    return datetime(today.year + 1, 4, 1)


async def get_incremental_budget(session, resource, last_sync_time, extra_query=None):
    """
    Retrieves budgets incrementally using a yearly interval approach.
    """
    extra_query = extra_query or {}
    final_date = compute_final_date()
    tokens = load_tokens()

    for tenant in tokens["tenants"]:
        logger.info(f"Processing tenant: {tenant['name']}")
        tenant_id = tenant["id"]
        budgets, _, _ = await get_request_budget(
            session, resource, tenant_id, last_sync_time, {}
        )

        for budget in budgets:
            budget_id = budget.get("BudgetID")
            if not budget_id:
                continue

            current_from = datetime.strptime(INITIAL_START_DATE, "%Y-%m-%d")

            while current_from < final_date:
                current_to = min(current_from + relativedelta(years=1), final_date)
                query_params = {
                    **extra_query,
                    "DateFrom": current_from.strftime("%Y-%m-%d"),
                    "DateTo": current_to.strftime("%Y-%m-%d"),
                }

                interval_result, _, _ = await get_request_budget(
                    session,
                    f"Budgets/{budget_id}",
                    tenant_id,
                    last_sync_time,
                    query_params,
                )

                yield interval_result
                current_from = current_to


async def process_budget_lines(budget_records, mdata):
    import logging

    logger = logging.getLogger(__name__)
    processed_records = []

    def get_parent_key(mdata_list):
        # Look for the metadata entry with an empty breadcrumb to get the parent's key property.
        for item in mdata_list:
            if not item.get("breadcrumb"):
                return item.get("metadata", {}).get("table-key-properties", ["ID"])[0]
        return "ID"

    parent_key = get_parent_key(mdata)
    logger.info(f"Determined parent key from mdata: {parent_key}")

    for budget in budget_records:
        parent_id = budget.get(parent_key)
        if not parent_id:
            logger.warning(f"Skipping budget record without {parent_key}: {budget}")
            continue

        budget_lines = budget.get("BudgetLines", [])
        if not budget_lines:
            logger.info(f"No BudgetLines found for budget {parent_id}")
            continue

        # Iterate over each budget line
        for line in budget_lines:
            # Check for BudgetBalances in the budget line
            balances = line.get("BudgetBalances", [])
            if not balances:
                logger.info(
                    f"No BudgetBalances found for a BudgetLine in budget {parent_id}"
                )
                continue

            # For each balance, create a flattened record.
            for balance in balances:
                flat_line = line.copy()
                # Remove the nested "BudgetBalances" field.
                flat_line.pop("BudgetBalances", None)
                # Merge the balance fields into the flat_line record.
                flat_line.update(balance)
                # Inject the parent's ID into the flattened record.
                flat_line["parent_id"] = parent_id
                processed_records.append(("budgets_lines", flat_line))
                logger.info(f"Processed flattened budget line for parent {parent_id}")

    return processed_records


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


def parse_xero_date(date_str):
    """
    Parse a Xero date string and convert it to ISO 8601 format with milliseconds.
    Example output: "2025-03-27T03:23:07.845Z"
    """
    if date_str.startswith("/Date(") and date_str.endswith(")/"):
        inner = date_str[6:-2]  # Extract the timestamp
        if "+" in inner:
            millis = int(inner.split("+")[0])
        elif "-" in inner:
            millis = int(inner.split("-")[0])
        else:
            millis = int(inner)
        dt = datetime.utcfromtimestamp(millis / 1000.0)
    else:
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")  # ISO format

    # Convert to ISO 8601 format with milliseconds and 'Z' for UTC
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def build_query_string(dict):
    if len(dict) == 0:
        return ""

    return "?" + "&".join(["{}={}".format(k, v) for k, v in dict.items()])


def write_record(row, resource, schema, mdata, dt):
    # Singer 6.0.1 breaks the transform step with an import error, so I've done a basic transform step below it (probably much worse but close enough for now).
    # Singer 6.1.0 fixes this but causes a version conflict when trying to install (clashes with Meltano's SDK which target-mssql is using)

    # with singer.Transformer() as transformer:
    #     mdata_dict = metadata.to_map(mdata)  # Convert list to dict
    #     rec = transformer.transform(row, schema, metadata=mdata_dict)
    # singer.write_record(resource, rec, time_extracted=dt)

    rec = {k: v for (k, v) in row.items() if k in schema["properties"]}
    singer.write_record(resource, rec)


# More convenient to use but has to all be held in memory, so use write_record instead for resources with many rows
def write_many(rows, resource, schema, mdata, dt):
    # with metrics.record_counter(resource) as counter:
    for row in rows:
        write_record(row, resource, schema, mdata, dt)
        # counter.increment()
