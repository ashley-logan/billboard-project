import httpx
import certifi
import asyncio
import datetime as dt
import time
import pandas as pd
from lxml import etree, HTMLPullParser

httpx_stream_config = {
    "timeout": {
        "connect": 5.0,
        "read": 10.0,
        "pool": 10.0,
    },
    "retry_strategy": {
        "max_attempts": 3,
        "backoff_factor": 0.25,
        "statuses": {502, 503, 504, 429},
        "methods": {"GET"},
        "retry_exceptions": True,
    },
    "client": {
        "base_url": "https://www.billboard.com/charts/hot-100/",
        "verify": certifi.where(),
    },
}

parser_config = {
    "num_scrapes": 100,
    "events": ("start", "end",),
    "parent_ele": {
        "tag": "ul",
        "class_": "o-chart-results-list-row //",
    },
    "target_eles": {
        "tags": ["h3", "span"]
    },
    "parser_ignore": [
        "NEW", "RE-\nENTRY",
    ]
}


def main():
    print("Hello from hot100-project!")
    instances = {
        "queue": asyncio.Queue(),
        "stream_client": ThrottledClient(**httpx_stream_config),
        "semaphore": asyncio.Semaphore(15),
        "parser": HTMLTargetParser,
        "parser_config": parser_config,
    }
    asyncio.run(extract())

if __name__ == "__main__":
    main()
