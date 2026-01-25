import asyncio
from platformdirs import PlatformDirs
import json
import os
from hot100_pkg.etl import extract, write_db

dirs = PlatformDirs("BillboardRepo")
cache_dir = dirs.user_cache_dir


def main():
    if os.path.isdir(cache_dir):
        # check most recent scrape date
        pass
    else:
        # scrape all records
        pass
    new_date, data = asyncio.run(extract())
    write_db(data)


if __name__ == "__main__":
    main()
