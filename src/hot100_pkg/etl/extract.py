import asyncio
from datetime import date, timedelta
import time
import re
from typing import Iterator
from httpx import AsyncClient, Response
from selectolax.parser import HTMLParser
from hot100_pkg.database import Charts, Entries
from hot100_pkg.utils import to_saturday


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


def date_generator(start_date: date, end_date: date) -> Iterator[date]:
    # infintite generator for dates incremented by one week
    curr: date = start_date
    while curr <= end_date:
        yield curr
        curr += timedelta(days=7)


async def clean_worker(queue2: asyncio.Queue, num_workers: int) -> list[Charts]:
    i: int = 0
    charts_list: list[Charts] = []
    while i < num_workers:
        chart_data: Charts | None = await queue2.get()
        print("pulled from queue2 ", i)
        if chart_data is None:
            i += 1
            continue
        charts_list.append(chart_data)
    return charts_list


async def scrape_worker(
    num: int,
    queue1: asyncio.Queue,
    queue2: asyncio.Queue,
    client: AsyncClient,
):
    i: int = 0
    print(f"Worker #{num} started")
    while True:
        item: tuple[date, str] | None = await queue1.get()
        if item is None:
            break
        date_, tail_url = item
        r: Response = await client.get(tail_url)
        charts: Charts = await parse_html(r.content, date_)
        print(f"putting #{i} into queue2")
        await queue2.put(charts)
        print(f"successfully put #{i} into queue2")
        i += 1
    print(f"Worker #{num} done; {i} charts parsed")
    await queue2.put(None)


async def parse_html(r_bytes: bytes, date_: date) -> Charts:
    entries: list[Entries] = []
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
            artist_txt = re.sub(r"(?<! )[aA]nd", " And", artist.text(strip=True))
            entries.append(
                Entries(
                    position=int(position.text(strip=True)),
                    title=title.text(strip=True)
                    .replace("RE-\nENTRY", "")
                    .replace("NEW", ""),
                    artist=artist_txt,
                )
            )
        if len(entries) >= 100:
            break
        num += 1
    chart: Charts = Charts(name="Hot 100", date=date_, entries=entries)
    return chart


async def url_producer(dates: Iterator[date], queue1: asyncio.Queue):
    for _date in dates:
        tail_url: str = f"{_date.strftime('%Y-%m-%d')}/"
        await queue1.put((_date, tail_url))
    for _ in range(15):
        await queue1.put(None)


async def extract(
    start_date: date, end_date: date | None = None
) -> tuple[list[Charts], date]:
    if not end_date:
        end_date = date.today()
    end_date = to_saturday(end_date)

    start_time: float = time.time()
    queue1: asyncio.Queue = asyncio.Queue(maxsize=15)
    queue2: asyncio.Queue = asyncio.Queue()
    client: AsyncClient = AsyncClient(
        base_url="https://www.billboard.com/charts/hot-100/", timeout=10.0
    )
    async with asyncio.TaskGroup() as tg:
        dates: Iterator[date] = date_generator(start_date, end_date)
        tg.create_task(url_producer(dates, queue1))
        for i in range(15):
            tg.create_task(scrape_worker(i + 1, queue1, queue2, client))
        result_task = tg.create_task(clean_worker(queue2, 15))

    await client.aclose()
    print(f"extract completed in {time.time() - start_time} seconds")

    return result_task.result(), end_date


# if __name__ == "__main__":
#     r = asyncio.run(extract())
#     print(r)
