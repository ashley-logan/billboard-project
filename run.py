import asyncio
from hot100_pkg.utils import create_parser, handle_args
from hot100_pkg.etl import extract


def main():
    parser = create_parser()
    args = parser.parse_args()
    charts_to_fetch = handle_args(args)

    asyncio.run(extract("hot-100", charts_to_fetch))


if __name__ == "__main__":
    main()
