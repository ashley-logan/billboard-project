import asyncio
import yaml
from hot100_pkg.etl import extract, transform, load
from hot100_pkg.utils import date_range_to_scrape, RAW_PATH


with open("configs/config_etl.yaml") as f:
    configs = yaml.safe_load(f)


async def main():
    await extract(
        configs["Client"], configs["Parser"], date_range_to_scrape(), RAW_PATH
    )
    # load(transform(RAW_PATH))


if __name__ == "__main__":
    asyncio.run(main())
