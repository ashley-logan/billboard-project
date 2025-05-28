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



async def clean_data(queue, **kwargs) -> list[dict[str, Any]]:
    output = []
    while True:
        raw_data = await queue.get()
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


async def scrape_data(dates, queue, stream_client, semaphore, **kwargs):
    retry_client = ThrottledClient(**httpx_stream_config)
    async with (
        stream_client,
        semaphore,
        asyncio.TaskGroup() as tg,
    ):
        for date in dates:
            config = {
                "client": retry_client,
                "parser": etree.HTMLPullParser(events=("start", "end")),
                "handler": HTMLTarget(),
                "output_queue": output_queue,
            }
            tg.create_task(html_driver(date, queue, **kwargs))

    await output_queue.put(None)


# testing parser feed() and read_events() methods
async def html_driver(date, queue, stream_client, parser, parser_config, **kwargs):
    url = date.strftime("%Y-%m-%d") + "/"
    targeted_parser = parser(**parser_config)
    await asyncio.sleep(0.2)
    async with stream_client.stream("GET", url) as response:
        async for chunk in response.aiter_text():
            # events_ = events_generator(parser, handler)
            targeted_parser.feed(chunk)
            if targeted_parser.at_quota:
                break
    data = targeted_parser.get_data()
    #
    await queue.put((date, data))


async def extract(**kwargs):
    start = time.time()
    dates = date_generator()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(scrape_data(dates, **kwargs))
        clean_task = tg.create_task(clean_data(**kwargs))

    data = clean_task.result()
    df = pd.DataFrame.from_records(data)
    df.to_parquet("hot100.parquet", engine="fastparquet")
    print(len(df))


if __name__ == "__main__":
    asyncio.run(extract())
