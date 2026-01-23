import asyncio
from datetime import date, timedelta
import time
from typing import Iterator
from httpx import AsyncClient, Response
from selectolax.parser import HTMLParser
from hot100_pkg.etl import Charts, Entries

# from hot100_pkg.utils import ThrottledClient, HTMLTargetParser


OLDEST_RECORD_DATE: date = date(1958, 8, 4)


def get_selectors(num: int) -> tuple[str, str, str]:
    # returns css selectors for:
    #   chart position, song title, artist
    return (
        f"""
        div.o-chart-results-list-row-container:nth-child({num}) > 
        ul:nth-child(1) > li:nth-child(1) > span:nth-child(1)
        """,
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
    # rounds a date to the previous saturday (billboard update day)
    while day.weekday() != 5:
        day -= timedelta(days=1)
    return day


def date_generator(start_date: date, end_date: date) -> Iterator[date]:
    # infintite generator for dates incremented by one week
    curr: date = start_date
    while curr <= end_date:
        yield curr
        curr += timedelta(days=7)


async def clean_worker(
    num: int, queue2: asyncio.Queue
) -> list[tuple[date, list[Entries]]]:
    i = 0
    resulting_data: list[tuple[date, list[Entries]]] = []
    while True:
        chart_data: tuple[date, list[Entries]] | None = await queue2.get()
        if chart_data is None:
            break
        resulting_data.append(chart_data)
        i += 1
        print(f"cleaned chart {i} in worker {num}")
    return resulting_data


# async def clean(
#     raw_data: list[list[str]],
# ) -> list[dict[int, str]]:
#     idxs: list[int] = [0, 2, 3]
#     col_names: list[str] = ["position", "date", "song", "artist"]
#     clean_data: list = []
#     for position, entry in enumerate(raw_data, start=1):
#         row_ = [position] + [entry[i] for i in idxs]
#         row_ = {col: data for col, data in zip(col_names, row_)}
#         clean_data.append(row_)
#     return clean_data


async def scrape_worker(
    num: int,
    queue1: asyncio.Queue,
    queue2: asyncio.Queue,
    client: AsyncClient,
):
    i: int = 0
    while True:
        item: tuple[date, str] | None = await queue1.get()
        if item is None:
            break
        _date, tail_url = item
        r: Response = await client.get(tail_url)
        chart_arr: list[Entries] = await parse_html(r.content)
        await queue2.put((_date, chart_arr))
        i += 1
        print(f"succesfully queued raw data {i} in worker {num}")
    await queue2.put(None)


async def parse_html(r_bytes: bytes) -> list[Entries]:
    chart_arr: list[Entries] = []
    tree: HTMLParser = HTMLParser(r_bytes)
    num: int = 1
    while True:
        position_css, title_css, artist_css = get_selectors(num)

        position, title, artist = (
            tree.css_first(position_css),
            tree.css_first(title_css),
            tree.css_first(artist_css),
        )
        if position and title and artist:
            chart_arr.append(
                Entries(
                    position=int(position.text(strip=True)),
                    title=title.text(strip=True)
                    .replace("RE-\nENTRY", "")
                    .replace("NEW", ""),
                    artist=artist.text(strip=True),
                )
            )
        if len(chart_arr) >= 100:
            break
        num += 1
    return chart_arr


async def url_producer(dates: Iterator[date], queue1: asyncio.Queue):
    for _date in dates:
        tail_url: str = f"{_date.strftime('%Y-%m-%d')}/"
        await queue1.put((_date, tail_url))
    for _ in range(10):
        await queue1.put(None)


# def dump_parquet(data: list[list], raw_data_path):
#     timestamp = dt.date.today().strftime("%m-%d")
#     parquet_path = f"{raw_data_path}/raw-data_{timestamp}.parquet"
#     pl.DataFrame(
#         data,
#         schema={
#             "position": pl.UInt8,
#             "date": pl.Date,
#             "song": pl.String,
#             "artist": pl.String,
#         },
#     ).write_parquet(parquet_path, compression="snappy", use_pyarrow=True)
#     print(f"wrote parquet file to {parquet_path}")


async def extract() -> list[list[tuple[date, list[Entries]]]]:
    start: float = time.time()
    queue1: asyncio.Queue = asyncio.Queue(maxsize=15)
    queue2: asyncio.Queue = asyncio.Queue()
    client: AsyncClient = AsyncClient(
        base_url="https://www.billboard.com/charts/hot-100/"
    )
    async with asyncio.TaskGroup() as tg:
        dates: Iterator[date] = date_generator(
            OLDEST_RECORD_DATE, into_saturday(date.today())
        )
        tg.create_task(url_producer(dates, queue1))
        for i in range(50):
            tg.create_task(scrape_worker(i, queue1, queue2, client))
        tasks = [tg.create_task(clean_worker(i, queue2)) for i in range(10)]
    client.aclose()
    return [t.result() for t in tasks]

    # dump_parquet([row for batch in batches for row in batch.result()], raw_data_path)
    print(f"script finished in {time.time() - start} seconds")


if __name__ == "__main__":
    r = asyncio.run(extract())
    print(r)
