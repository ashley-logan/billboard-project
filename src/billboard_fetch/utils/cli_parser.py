from billboard_fetch.utils import date_generator
from billboard_fetch.configs import OLDEST_CHART_DATE, DB_URI, CHARTS, CHART_INFO
from billboard_fetch.database import Chart, Base
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker
from datetime import date, datetime
from argparse import ArgumentParser, ArgumentTypeError, Namespace
from typing import Iterator, Optional


engine = create_engine(DB_URI)

Base.metadata.create_all(engine)  # create all tables defined in models.py

Session = sessionmaker(engine)


def parse_name(name_arg: str) -> CHART_INFO:
    name_arg = name_arg.lower()
    guesses = []
    for c in CHARTS:
        if name_arg == c.name:
            return c
        elif name_arg in c.alt_names:
            return c
        elif name_arg in c.name:
            guesses.append(c.name)

    if guesses:
        raise ArgumentTypeError(
            f"""
                {name_arg} is not a valid option for --chart; perhaps you meant one of {guesses}?
            """
        )
    else:
        raise ArgumentTypeError(
            f"""
            {name_arg} is not a valid option for --chart; refer to CHARTS.md for compatible charts
            """
        )


def parse_date(date_arg: str) -> date:
    if date_arg == "OLDEST":
        return OLDEST_CHART_DATE

    if date_arg == "TODAY":
        return date.today()

    try:
        value: date = datetime.strptime(date_arg, "%Y-%m-%d").date()
        if value < OLDEST_CHART_DATE or value > date.today():
            raise ValueError
        return value
    except ValueError:
        raise ArgumentTypeError(
            f"""
            Invalid argument {date_arg}. Argument must be one of the following:
            1) date in the form 'YYYY-MM-DD' which must be >= 1958-08-02
            2) 'OLDEST' which represents the earliest chart (1958-08-02)
            3) 'TODAY' which represents the most recent chart
            """
        )


def create_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description="Entrypoint script for the Billboard fetch utility",
        epilog="Example fetching all charts: docker run billboard-fetcher  --all \nExample fetching a month of charts: docker run billboard-fetcher fetch-range --start 2004-03-21 --end 2004-04-21",
    )

    pattern_flag = parser.add_mutually_exclusive_group()

    parser.add_argument(
        "--chart",
        type=parse_name,
        required=True,
        help="Specifies the billboard chart to fetch. Some choices include ('hot-100' 'artist-100', 'streaming-songs'). For all valid options see CHARTS.md",
    )
    parser.add_argument(
        "--start",
        type=parse_date,
        required=False,
        default="OLDEST",
        help="The oldest chart to fetch, pass a date in the form 'YYYY-MM-DD' or defaults to the oldest chart (1958-08-02)",
    )

    parser.add_argument(
        "--end",
        type=parse_date,
        required=False,
        default="TODAY",
        help="The newest chart to fetch, pass a date in the form 'YYYY-MM-DD' or defaults to today's date",
    )

    pattern_flag.add_argument(
        "--new",
        action="store_true",
        help="Fetch all charts newer than the the most recent chart in database. Database must have at least one chart stored for this argument to be passed.",
    )
    pattern_flag.add_argument(
        "--missing",
        action="store_true",
        help="Fetch all charts that are not currently stored in the database. If database is empty this is the same as --all",
    )
    pattern_flag.add_argument(
        "--older",
        action="store_true",
        help="Fetch all charts older than the oldest chart in the databse. Database must have at least one chart stored to pass this argument.",
    )
    pattern_flag.add_argument(
        "--all",
        action="store_true",
        help="Fetches all Chart and automatically overwrites and existing database entries",
    )

    pattern_flag.add_argument(
        "--single",
        type=parse_date,
        help="Fetches only the chart from the specified data. This flag takes precendence over all other arguments",
    )

    # fetch_range.add_argument(
    #     "--allow-overwrite",
    #     action="store_true",
    #     help="Allow overwriting chart data that already exists in database",
    # )

    return parser


def parse_flags(parser: ArgumentParser) -> tuple[CHART_INFO, Iterator[date]]:
    try:
        args: Namespace = parser.parse_args()

        if args.single:  # --single takes precedence, immediately propagate target date
            return args.chart, date_generator(args.single, args.single)

        if args.start > args.end:  # date range sanity check
            raise parser.error(message="--start cannot be recent than --end")

        elif (
            args.start == args.end
        ):  # encourage the use of --single flag if only targeting one chart
            print("Warning: to parse a single chart use '--single [YYYY-MM-DD]")

        if args.all:  # targeting every chart in the optional range
            return args.chart, date_generator(args.start, args.end)

        elif (
            args.missing
        ):  # targeting charts not in the database and in the optional range
            with Session() as s:
                existing = s.scalars(select(Chart.date)).all()
            return args.chart, date_generator(
                args.start, args.end, lambda x: x not in existing
            )

        elif args.new:  # targeting charts newer than the newest chart in the database and within the optional range
            with Session() as s:
                newest: Optional[date] = s.scalars(
                    select(func.max(Chart.date))
                ).one_or_none()

            if (
                newest is None
            ):  # --new is relative to the database so it must contain at least one chart
                raise parser.error(
                    message="Cannot pass --new when no charts currently exist in the database."
                )

            elif args.end <= newest:  # functional date range sanity check
                raise parser.error(
                    message=f"the newest chart cannot be newer than the --end constraint. you passed --end={args.end} while newest={newest}"
                )

            return args.chart, date_generator(
                args.start, args.end, lambda x: x > newest
            )

        elif args.older:  # targeting charts older than the oldest chart in the database and within the optional range
            with Session() as s:
                oldest: Optional[date] = s.scalars(
                    select(func.min(Chart.date))
                ).one_or_none()

            if (
                oldest is None
            ):  # --older is relative to the database so it must contain at least one chart
                raise parser.error(
                    message="Cannot pass --older when no charts currently exist in the database."
                )

            elif args.start >= oldest:  # functional date range sanity check
                raise parser.error(
                    message=f"the oldest chart cannot be older than the --start constraint. you passed --start={args.start} while oldest={oldest}"
                )
            return args.chart, date_generator(
                args.start, args.end, lambda x: x < oldest
            )
        else:  # if no pattern flag is passed use specified date range or default
            return args.chart, date_generator(args.start, args.end)

    finally:
        engine.dispose()  # close database connection
