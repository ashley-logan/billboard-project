import asyncio
import yaml
from pathlib import Path
from etl import extract, transform, load
from utils import date_range_to_scrape

with open("config.yaml") as f:
    configs = yaml.safe_load(f)

root_dir = Path(__file__).parent
db_output_folder = root_dir / "data" / "processed_data"
db_output_folder.mkdir(parents=True, exist_ok=True)
raw_output_folder = root_dir / "data" / "raw_data"
raw_output_folder.mkdir(parents=True, exist_ok=True)
DB_PATH = db_output_folder / "chart-analytics.duckdb"
RAW_PATH = raw_output_folder


async def main():
    date_range = date_range_to_scrape(db_path=DB_PATH)
    await extract(configs["Client"], configs["Parser"], date_range, RAW_PATH)
    clean_df = transform(load_path=RAW_PATH)
    load(clean_df, DB_PATH)


if __name__ == "__main__":
    asyncio.run(main())
