import httpx
import certifi
import asyncio
import datetime as dt
import time
import pandas as pd
from lxml import etree
from typing import Any, Generator
from handler_class import HTMLHandler, ThrottledClient


OLDEST = dt.datetime(1958, 8, 4)

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


class HTMLTarget(HTMLHandler):
    PARENT_TAGS = ["ul"]  # parent tag that contains all relevant nodes
    CHILD_TAGS = ["span", "h3"]  # tags within parent node that contain relevant data
    PARENT_CLASSES = ["o-chart-results-list-row //"]  # beginning of the parent class
    TEXT_IGNORE = ["NEW", "RE-\nENTRY"]


def normalize_to_sat(func):
    def wrapper(*args, **kwargs):
        if not args and not kwargs:
            date = dt.datetime.today()
        else:
            date = kwargs.get("latest_date") if "latest_date" in kwargs else args[0]

        if not isinstance(date, dt.datetime):
            raise TypeError(f"Expected datetime object, got {type(date).__name__}")
        while date.weekday() != 5:
            date += dt.timedelta(days=1)
        kwargs["date"] = date
        return func(*args, **kwargs)

    return wrapper


@normalize_to_sat
def date_generator(date=dt.datetime.today(), delta=1):
    # infintite generator for dates that takes the timedelta as a paramater
    curr = date
    while curr >= OLDEST:
        yield curr
        curr -= dt.timedelta(weeks=delta)


def events_generator(
    parser, handler, max_len=100
) -> Generator[tuple[str, etree.Element], None, None]:
    events = parser.read_events()
    for event, ele in events:
        if len(handler) >= max_len:
            break
        yield event, ele


async def clean_data(input_queue) -> list[dict[str, Any]]:
    output = []
    while True:
        raw_data = await input_queue.get()
        if raw_data is None:
            break
        clean_data = await clean(raw_data)
        output.extend(clean_data)
    return output


async def clean(page) -> list[dict[str, Any]]:
    idxs = [1, 2, 5]
    col_names = ["date", "position", "song", "artist", "wks_on_chart"]
    date, raw_data = page
    clean_data = []
    for position, entry in enumerate(raw_data, start=1):
        row_ = [date, position] + [entry[i] for i in idxs]
        row_ = {col: data for col, data in zip(col_names, row_)}
        clean_data.append(row_)

    return clean_data


async def scrape_data(time_, generator, output_queue):
    retry_client = ThrottledClient(**httpx_stream_config)
    async with (
        retry_client,
        asyncio.Semaphore(10),
        asyncio.TaskGroup() as tg,
    ):
        for date in generator:
            config = {
                "client": retry_client,
                "parser": etree.HTMLPullParser(events=("start", "end")),
                "handler": HTMLTarget(),
                "output_queue": output_queue,
            }
            tg.create_task(html_driver(time_, date, **config))

    await output_queue.put(None)


# testing parser feed() and read_events() methods
async def html_driver(time_, date, client, parser, handler, output_queue):
    url = date.strftime("%Y-%m-%d") + "/"
    await asyncio.sleep(0.2)
    async with client.stream("GET", url) as response:
        async for chunk in response.aiter_text():
            events_ = events_generator(parser, handler)
            parser.feed(chunk)
            for event, ele in events_:
                handler(event, ele)
            if len(handler) >= 100:
                break
    content = handler.get_content()
    #
    await output_queue.put((date, content))


# async def timer():
#     start = time.time()
#     while True:
#         await asyncio.sleep(30)
#         print(f"{time.time() - start} seconds")


# @timing_decorator
async def main():
    start = time.time()
    queue1 = asyncio.Queue()
    dates = date_generator()
    # timer_task = asyncio.create_task(timer())

    async with asyncio.TaskGroup() as tg:
        # logging.info(
        #     "initalized TaskGroup",
        #     extra={"second": time.time() - start, "variable": None},
        # )
        tg.create_task(scrape_data(time_=start, generator=dates, output_queue=queue1))
        clean_task = tg.create_task(clean_data(input_queue=queue1))

    # timer_task.cancel()
    data = clean_task.result()
    df = pd.DataFrame.from_records(data)
    df.to_parquet("hot100.parquet", engine="fastparquet")
    print(len(df))


if __name__ == "__main__":
    asyncio.run(main())
