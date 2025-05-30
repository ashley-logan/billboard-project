import asyncio
import datetime as dt
import time
import json
from typing import Generator
from pathlib import Path
from client_class import ThrottledClient
from parser_class import HTMLTargetParser


OLDEST = dt.datetime(1958, 8, 4)


def normalize_to_sat(func):
    def wrapper(*args, **kwargs):
        if not args and not kwargs:
            date = dt.datetime.today()
        else:
            date = kwargs.get("date") if "date" in kwargs else args[0]

        if not isinstance(date, dt.datetime):
            raise TypeError(f"Expected datetime object, got {type(date).__name__}")
        while date.weekday() != 5:
            date += dt.timedelta(days=1)
        kwargs["date"] = date
        return func(*args, **kwargs)

    return wrapper


@normalize_to_sat
def date_generator(
    date: dt.datetime = dt.datetime.today(), delta: int = 1
) -> Generator[dt.datetime, None, None]:
    # infintite generator for dates that takes the timedelta as a paramater
    curr = date
    while curr >= OLDEST:
        yield curr
        curr -= dt.timedelta(weeks=delta)


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
    idxs = [0, 2, 3, 6]
    col_names = ["position", "date", "song", "artist", "wks_on_chart"]
    clean_data = []
    for position, entry in enumerate(raw_data, start=1):
        row_ = [position] + [entry[i] for i in idxs]
        row_ = {col: data for col, data in zip(col_names, row_)}
        clean_data.append(row_)
    print(clean_data)
    return clean_data


async def scrape_worker(
    num: int,
    queue1: asyncio.Queue,
    queue2: asyncio.Queue,
    client: ThrottledClient,
    semaphore: asyncio.Semaphore,
    parser_kwargs: dict,
):
    i = 0
    while True:
        item = await queue1.get()
        if item is None:
            break
        date, tail_url = item
        async with semaphore:
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
    data = parser.get_data()
    print(f"parsed data from {date.date().isoformat()}")
    return [[date.date().isoformat()] + row for row in data]


async def url_producer(dates: Generator, queue1: asyncio.Queue):
    for date in dates:
        tail_url = f"{date.strftime('%Y-%m-%d')}/"
        await queue1.put((date, tail_url))
    for _ in range(10):
        await queue1.put(None)


def dump_json(data: list[list]):
    curr_dir = Path.cwd()
    output_folder = curr_dir / "hot100" / "hot100_datafiles"
    timestamp = dt.datetime.now()
    date = timestamp.strftime("%m-%d_%H-%M")
    filename = f"records{date}.json"
    filepath = output_folder / filename
    output_folder.mkdir(exist_ok=True)

    with filepath.open("w") as f:
        json.dump(data, f)
    print(f"wrote json file to {filepath}")


async def extract(
    client_config: dict, semaphore: asyncio.Semaphore, parser_config: dict
):
    start = time.time()
    queue1 = asyncio.Queue(maxsize=15)
    queue2 = asyncio.Queue()
    dates = date_generator()
    client = ThrottledClient(**client_config)
    batches = []
    async with asyncio.TaskGroup() as tg:
        tg.create_task(url_producer(dates, queue1))
        for i in range(10):
            tg.create_task(
                scrape_worker(i, queue1, queue2, client, semaphore, parser_config)
            )

        for i in range(10):
            batches.append(tg.create_task(clean_worker(i, queue2)))

    dump_json([row for batch in batches for row in batch.result()])
    print(f"script finished in {time.time() - start} seconds")


if __name__ == "__main__":
    asyncio.run(extract())
