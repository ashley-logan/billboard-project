import asyncio
from datetime import date, timedelta
import time
import polars as pl
from typing import Generator, Callable
import httpx
from selectolax.parser import HTMLParser
from hot100_pkg.utils import ThrottledClient, HTMLTargetParser


def get_selectors(num: int) -> tuple[str, str]:
    return (
        f"""
    div.o-chart-results-list-row-container:nth-child({num}) > 
    ul:nth-child(1) > li:nth-child(4) > ul:nth-child(1) > 
    li:nth-child(1) > h3:nth-child(1)
    """,
        f"""
    div.o-chart-results-list-row-container:nth-child({num}) > 
    ul:nth-child(1) > li:nth-child(4) > ul:nth-child(1) > 
    li:nth-child(1) > span:nth-child(2)
    """,
    )


def into_saturday(day: date) -> date:
    while day.weekday() != 5:
        day -= timedelta(days=1)
    return day


def date_generator(start_date: date, end_date: date):
    # infintite generator for dates that takes the timedelta as a paramater
    curr = start_date
    while curr <= end_date:
        yield curr
        curr += timedelta(days=7)


async def clean_worker(num: int, queue2: asyncio.Queue) -> list[dict[int, str]]:
    i = 0
    resulting_data = []
    while True:
        raw_data = await queue2.get()
        if raw_data is None:
            break
        clean_data = await clean(raw_data)
        resulting_data.extend(clean_data)
        i += 1
        print(f"cleaned data batch {i} in worker {num}")
    return resulting_data


async def clean(
    raw_data: list[list[str]],
) -> list[dict[int, str]]:
    idxs: list[int] = [0, 2, 3]
    col_names: list[str] = ["position", "date", "song", "artist"]
    clean_data: list = []
    for position, entry in enumerate(raw_data, start=1):
        row_ = [position] + [entry[i] for i in idxs]
        row_ = {col: data for col, data in zip(col_names, row_)}
        clean_data.append(row_)
    return clean_data


async def scrape_worker(
    num: int,
    queue1: asyncio.Queue,
    queue2: asyncio.Queue,
    client: ThrottledClient,
    parser_kwargs: dict,
):
    i = 0
    while True:
        item = await queue1.get()
        if item is None:
            break
        date, tail_url = item
        async with asyncio.Semaphore(15):
            raw_data = await html_driver(date, tail_url, client, parser_kwargs)
        await queue2.put(raw_data)
        i += 1
        print(f"succesfully queued raw data {i} in worker {num}")
    await queue2.put(None)


async def parse_html(r_bytes: bytes) -> list[tuple[str, str]]:
    chart_data = []
    tree: HTMLParser = HTMLParser(r_bytes)
    for i in range(1, 101):
        title_selector, artist_selector = get_selectors(i)
        chart_data.append(
            (
                tree.css_first(title_selector).text(),
                tree.css_first(artist_selector).text(),
            )
        )
    return chart_data


async def html_driver(
    date: date, tail_url: str, client: ThrottledClient
) -> list[tuple[str, str]]:
    data = []
    async with client.stream("GET", tail_url) as response:
        data = parse_html(response.content)
    print(f"parsed data from {date.isoformat()}")
    return data


async def url_producer(dates: Generator, queue1: asyncio.Queue):
    for date in dates:
        tail_url = f"{date.strftime('%Y-%m-%d')}/"
        await queue1.put((date, tail_url))
    for _ in range(10):
        await queue1.put(None)


def dump_parquet(data: list[list], raw_data_path):
    timestamp = dt.date.today().strftime("%m-%d")
    parquet_path = f"{raw_data_path}/raw-data_{timestamp}.parquet"
    pl.DataFrame(
        data,
        schema={
            "position": pl.UInt8,
            "date": pl.Date,
            "song": pl.String,
            "artist": pl.String,
        },
    ).write_parquet(parquet_path, compression="snappy", use_pyarrow=True)
    print(f"wrote parquet file to {parquet_path}")


async def extract(
    client_config: dict,
    parser_config: dict,
    date_range: list[dt.date],
    raw_data_path: str,
):
    start: float = time.time()
    queue1: asyncio.Queue = asyncio.Queue(maxsize=15)
    queue2: asyncio.Queue = asyncio.Queue()
    client: ThrottledClient = ThrottledClient(**client_config)
    async with asyncio.TaskGroup() as tg:
        tg.create_task(url_producer(date_generator(*date_range), queue1))
        for i in range(10):
            tg.create_task(scrape_worker(i, queue1, queue2, client, parser_config))
        batches = [tg.create_task(clean_worker(i, queue2)) for i in range(10)]

    dump_parquet([row for batch in batches for row in batch.result()], raw_data_path)
    print(f"script finished in {time.time() - start} seconds")


# if __name__ == "__main__":
#     asyncio.run(extract())
