import aiohttp
import json
import os
import time
import singer

from .utility import RateLimiter
from .internal import do_sync, main_impl

logger = singer.get_logger()

TOKEN_FILE = "token_cache_xero.json"
TOKEN_URL = "https://identity.xero.com/connect/token"
TENANT_URL = "https://api.xero.com/connections"

REQUIRED_CONFIG_KEYS = ["client_id", "client_secret", "refresh_token"]


# Load tokens from cache if they exist.
def load_tokens():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            return json.load(f)
    return {}


# Save tokens with expiration timestamp and tenant details.
def save_tokens(access_token, refresh_token, expires_in, tenants=None):
    expiration = time.time() + expires_in if expires_in else 0  # Ensure it's never None
    token_data = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expiration": expiration,
        "tenants": tenants or [],
    }
    with open(TOKEN_FILE, "w") as f:
        json.dump(token_data, f)


# Check if the cached token is expired.
def is_token_expired():
    tokens = load_tokens()
    expiration = tokens.get("expiration", 0)  # Defaults to expired if missing
    return not expiration or time.time() >= expiration


# Refresh access token, always checking if cache exists.
async def refresh_access_token(config):
    tokens = load_tokens()

    # First run: Use config values
    if not tokens or "refresh_token" not in tokens:
        logger.info("No cached token found. Using config credentials for first run.")
        refresh_token = config["refresh_token"]
    else:
        refresh_token = tokens["refresh_token"]

    if not refresh_token:
        raise Exception("No refresh token available. Manual authentication required.")

    logger.info("Refreshing Xero token...")

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": config["client_id"],
        "client_secret": config["client_secret"],
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(TOKEN_URL, data=data) as response:
            if response.status == 200:
                new_tokens = await response.json()
                tenants = await get_tenant_ids(new_tokens["access_token"])
                save_tokens(
                    new_tokens["access_token"],
                    new_tokens["refresh_token"],
                    new_tokens["expires_in"],
                    tenants,
                )
                return new_tokens["access_token"], tenants
            else:
                raise Exception(f"Failed to refresh token: {await response.text()}")


# Retrieve a valid access token, refreshing if necessary.
async def get_access_token(config):
    tokens = load_tokens()

    if not tokens or is_token_expired():
        return await refresh_access_token(config)

    return tokens["access_token"], tokens.get("tenants", [])


# Fetch all tenant IDs linked to the Xero access token, with caching.
async def get_tenant_ids(access_token):
    tokens = load_tokens()

    # Ensure expiration is always a float
    expiration = tokens.get("expiration", 0)

    # Use cached tenants if available
    if "tenants" in tokens and tokens["tenants"]:
        return tokens["tenants"]

    logger.info("Fetching Xero tenant IDs...")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(TENANT_URL, headers=headers) as response:
            if response.status == 200:
                tenants = await response.json()
                tenant_list = [
                    {"id": tenant["tenantId"], "name": tenant["tenantName"]}
                    for tenant in tenants
                ]

                # Fix: Ensure expiration is never None
                expires_in = max(expiration - time.time(), 0)

                # Save updated tenants list in cache
                save_tokens(
                    tokens.get("access_token"),
                    tokens.get("refresh_token"),
                    expires_in,  # Ensure it's never None
                    tenant_list,
                )

                return tenant_list
            else:
                raise Exception(f"Error fetching tenants: {await response.text()}")


# Async ETL execution
async def run_async(config, state, catalog):
    api_token, tenants = await get_access_token(config)  # ✅ Calls function with config

    if api_token:
        logger.info(
            f"✅ Successfully authenticated. Tenants: {[t['name'] for t in tenants]}"
        )
    else:
        logger.error("❌ Failed to retrieve access token.")

    auth_headers = {"Authorization": f"Bearer {api_token}"}
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(headers=auth_headers, timeout=timeout) as session:
        session = RateLimiter(session)
        await do_sync(session, state, catalog)


# Main ETL entry point.
def main():
    main_impl(REQUIRED_CONFIG_KEYS, run_async)


if __name__ == "__main__":
    main()
