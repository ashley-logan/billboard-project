import asyncio
import yaml
from etl import extract, transform, load
from utils import DB_PATH, RAW_PATH, date_range_to_scrape

with open("config.yaml") as f:
    configs = yaml.safe_load(f)


async def main():
    date_range = date_range_to_scrape()
    await extract(configs["Client"], configs["Parser"], date_range, RAW_PATH)
    clean_df = transform(RAW_PATH)
    load(clean_df, DB_PATH)


if __name__ == "__main__":
    asyncio.run(main())
