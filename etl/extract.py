import asyncio
import datetime as dt
import time
import pyarrow
import polars as pl
from typing import Generator
from utils import ThrottledClient, HTMLTargetParser, round_date


@round_date
def date_generator(
    start_date: dt.date, end_date: dt.date, delta: int = 1
) -> Generator[dt.date, None, None]:
    # infintite generator for dates that takes the timedelta as a paramater
    curr = start_date
    while curr <= end_date:
        yield curr
        curr += dt.timedelta(weeks=delta)


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


# testing parser feed() and read_events() methods
async def html_driver(
    date: dt.datetime, tail_url: str, client: ThrottledClient, parser_config: dict
) -> list[list[str]]:
    parser = HTMLTargetParser(parser_config)
    async with client.stream("GET", tail_url) as response:
        async for chunk in response.aiter_text():
            parser.feed(chunk)
            if parser.at_quota:
                break
    data: list[list] = parser.get_data()
    print(f"parsed data from {date.isoformat()}")
    return [[date] + row for row in data]


async def url_producer(dates: Generator, queue1: asyncio.Queue):
    for date in dates:
        tail_url = f"{date.strftime('%Y-%m-%d')}/"
        await queue1.put((date, tail_url))
    for _ in range(10):
        await queue1.put(None)


def dump_parquet(data: list[list], extract_path):
    pl.DataFrame(
        data,
        schema={
            "position": pl.UInt8,
            "date": pl.Date,
            "song": pl.String,
            "artist": pl.String,
        },
    ).write_parquet(extract_path, compression="snappy", use_pyarrow=True)
    print(f"wrote parquet file to {extract_path}")


async def extract(
    client_config: dict,
    parser_config: dict,
    date_range: tuple[dt.date],
    extract_path: str,
):
    start: float = time.time()
    queue1: asyncio.Queue = asyncio.Queue(maxsize=15)
    queue2: asyncio.Queue = asyncio.Queue()
    client = ThrottledClient(**client_config)
    batches = []
    async with asyncio.TaskGroup() as tg:
        tg.create_task(url_producer(date_generator(*date_range), queue1))
        for i in range(10):
            tg.create_task(scrape_worker(i, queue1, queue2, client, parser_config))

        for i in range(10):
            batches.append(tg.create_task(clean_worker(i, queue2)))

    dump_parquet([row for batch in batches for row in batch.result()], extract_path)
    print(f"script finished in {time.time() - start} seconds")
    return file_name


# if __name__ == "__main__":
#     asyncio.run(extract())
