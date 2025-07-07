import asyncio
import yaml
import os
from src.etl import extract, transform, load
from src.utils import date_range_to_scrape, RAW_PATH


with open("configs/config_etl.yaml") as f:
    configs = yaml.safe_load(f)


async def main():
    date_range = date_range_to_scrape()
    await extract(configs["Client"], configs["Parser"], date_range, RAW_PATH)
    clean_df = transform(load_path=RAW_PATH)
    load(clean_df)


if __name__ == "__main__":
    # asyncio.run(main())
    print(os.getcwd())
