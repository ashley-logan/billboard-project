import asyncio
from platformdirs import PlatformDirs
from datetime import date, timedelta
import os
from pathlib import Path
from hot100_pkg.etl import extract
from hot100_pkg.database import write_db
from hot100_pkg.utils import load_cache, to_saturday, update_cache, OLDEST_RECORD_DATE

dirs = PlatformDirs("BillboardRepo")
cache_path: Path = Path(dirs.user_cache_path)


def main():
    start_date: date = OLDEST_RECORD_DATE
    if os.path.isfile(cache_path):
        cache = load_cache(cache_path)
        most_recent = date.fromisoformat(cache["most_recent_chart_date"])
        if most_recent < to_saturday(date.today()):
            start_date: date = most_recent + timedelta(days=7)
        else:
            print(f"No newer charts to fetch, most recent chart: {most_recent}")
            return

    data, newest_chart_date = asyncio.run(extract(start_date, date(1960, 11, 20)))
    records_added = write_db(data)
    update_cache(cache_path, newest_chart_date, records_added)


if __name__ == "__main__":
    main()
