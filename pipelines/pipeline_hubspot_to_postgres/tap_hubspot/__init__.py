import aiohttp
import singer


from .utility import RateLimiter
from .internal import do_sync, main_impl

logger = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["hubspot_token"]


async def run_async(config, state, catalog):
    api_token = config["hubspot_token"]

    auth_headers = {"Authorization": f"Bearer {api_token}"}
    # default is 300s, which is excessive
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(headers=auth_headers, timeout=timeout) as session:
        session = RateLimiter(session)
        await do_sync(session, state, catalog)


def main():
    main_impl(REQUIRED_CONFIG_KEYS, run_async)


if __name__ == "__main__":
    main()
