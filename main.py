import asyncio
from etl import extract, transform

throttled_client_config = {
    "timeout_config": {
        "connect": 5.0,
        "write": 5.0,
        "read": 10.0,
        "pool": 10.0,
    },
    "retry_config": {
        "total": 3,
        "max_backoff_wait": 15.0,
        "backoff_factor": 0.25,
        "allowed_methods": ["GET"],
    },
    "client_config": {
        "base_url": "https://www.billboard.com/charts/hot-100/",
    },
}

parser_config = {
    "num_scrapes": 100,
    "events": (
        "start",
        "end",
    ),
    "parent_ele": {
        "tag": "ul",
        "class_": "o-chart-results-list-row //",
    },
    "target_eles": {"tags": ["h3", "span"]},
    "parser_ignore": [
        "NEW",
        "RE-\nENTRY",
    ],
}


async def main():
    extract_config = {
        "client_config": throttled_client_config,
        "semaphore": asyncio.Semaphore(15),
        "parser_config": parser_config,
    }
    file_name = await extract(**extract_config)
    transform(file_name)


if __name__ == "__main__":
    asyncio.run(main())
