from hot100_pkg.utils import OLDEST_RECORD_DATE, date_generator
from hot100_pkg.database import Charts
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker
from datetime import date, datetime
import argparse
import os
from typing import Iterator, Optional
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_PORT = os.getenv("POSTGRES_PORT")

DB_URI = f"postgres+psycopg:///{DB_USER}:{DB_PASSWORD}@db:{DB_PORT}/{DB_NAME}"

engine = create_engine(DB_URI)

Session = sessionmaker(engine)


def parse_date(date_arg: str) -> date:
    if date_arg == "OLDEST":
        return OLDEST_RECORD_DATE

    if date_arg == "TODAY":
        return date.today()

    try:
        value: date = datetime.strptime(date_arg, "%Y-%m-%d").date()
        if value < OLDEST_RECORD_DATE or value > date.today():
            raise ValueError
        return value
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"""
            Invalid argument {date_arg}. Argument must be one of the following:
            1) date in the form 'YYYY-MM-DD' which must be >= 1958-08-02
            2) 'OLDEST' which represents the earliest chart (1958-08-02)
            3) 'TODAY' which represents the most recent chart
            """
        )


def parse_cli() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Entrypoint script for the Billboard fetch utility",
        epilog="Example fetching all charts: docker run billboard-fetcher fetch --all \nExample fetching a month of charts: docker run billboard-fetcher fetch-range --start 2004-03-21 --end 2004-04-21",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch = subparsers.add_parser("fetch")

    fetch_parser = fetch.add_mutually_exclusive_group(required=True)

    fetch_parser.add_argument(
        "--new",
        action="store_true",
        help="Fetch all charts newer than the the most recent chart in database. Database must have at least one chart stored for this argument to be passed.",
    )
    fetch_parser.add_argument(
        "--missing",
        action="store_true",
        help="Fetch all charts that are not currently stored in the database. If database is empty this is the same as --all",
    )
    fetch_parser.add_argument(
        "--older",
        action="store_true",
        help="Fetch all charts older than the oldest chart in the databse. Database must have at least one chart stored to pass this argument.",
    )
    fetch_parser.add_argument(
        "--all",
        action="store_true",
        help="Fetches all charts and automatically overwrites and existing database entries",
    )

    fetch_parser.add_argument(
        "--single",
        type=parse_date,
        help="Fetches only the chart from the specified data. Overwrites any chart from the specified data unless --allow-overwrite=False",
    )

    fetch_range = subparsers.add_parser("fetch-range")
    fetch_range.add_argument(
        "--start",
        type=parse_date,
        required=True,
        help="The oldest chart to fetch, pass a date in the form 'YYYY-MM-DD' or pass 'OLDEST' to get the oldest possible chart",
    )
    fetch_range.add_argument(
        "--end",
        type=parse_date,
        required=True,
        help="The newest chart to fetch, pass a date in the form 'YYYY-MM-DD' or pass 'TODAY' to get the newest possible chart",
    )

    # fetch_range.add_argument(
    #     "--allow-overwrite",
    #     action="store_true",
    #     help="Allow overwriting chart data that already exists in database",
    # )

    args = parser.parse_args()

    return args


def handle_args(args: argparse.Namespace) -> Iterator[date]:
    if args.command == "fetch-range":
        if args.start > args.end:
            raise argparse.ArgumentError(
                argument=None, message="--start cannot be recent than --end"
            )
        if args.start == args.end:
            print("Warning: to parse a single chart use 'fetch --single [YYYY-MM-DD]")
        dates = date_generator(args.start, args.end)
    elif args.all:
        dates = date_generator(OLDEST_RECORD_DATE, date.today())
    elif args.missing:
        with Session() as s:
            existing = s.scalars(select(Charts.date)).all()
        dates = date_generator(
            OLDEST_RECORD_DATE, date.today(), lambda x: x not in existing
        )
    elif args.new:
        with Session() as s:
            newest: Optional[date] = s.scalars(
                select(func.max(Charts.date))
            ).one_or_none()
        if newest is None:
            raise argparse.ArgumentError(
                argument=None,
                message="Cannot pass --new when no charts currently exist in the database.",
            )
        dates = date_generator(newest, date.today())
    elif args.older:
        with Session() as s:
            oldest: Optional[date] = s.scalars(
                select(func.min(Charts.date))
            ).one_or_none()
        if oldest is None:
            raise argparse.ArgumentError(
                argument=None,
                message="Cannot pass --older when no charts currently exist in the database.",
            )
    elif single_date := args.single:
        dates = date_generator(single_date, single_date)

    return dates
