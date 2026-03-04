import asyncio
from datetime import date, timedelta
from sqlalchemy import create_engine
from .entrypoint import parse_cli, handle_args
from hot100_pkg.etl import extract
from hot100_pkg.database import write_db, DB_URI
from hot100_pkg.utils import load_cache, to_saturday, update_cache, OLDEST_RECORD_DATE


def main():
    args = parse_cli()
    charts_to_fetch = handle_args(args)

    asyncio.run(extract("hot-100", charts_to_fetch))


if __name__ == "__main__":
    # print(os.path.isfile(cache_path))
    # os.remove(cache_path)
    # os.remove(DB_PATH)
    main()
