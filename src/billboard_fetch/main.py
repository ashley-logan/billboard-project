import asyncio
from billboard_fetch.utils import create_parser, handle_args
from billboard_fetch.etl import extract


def run():
    parser = create_parser()
    args = parser.parse_args()
    charts_to_fetch = handle_args(args)

    asyncio.run(extract("hot-100", charts_to_fetch))

