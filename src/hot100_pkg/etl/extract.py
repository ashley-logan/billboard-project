import asyncio
from datetime import date, timedelta
import time
import json
import re
from pathlib import Path
from typing import Iterator
from httpx import AsyncClient, Response
from aiohttp import ClientSession, TCPConnector
from selectolax.parser import HTMLParser, Node
from hot100_pkg.database import Charts, Entries
from hot100_pkg.utils import (
    to_saturday,
    AsyncCounter,
    calc_num_charts,
    progress_report,
    retry_middleware,
)

LOG_DIR = Path(__file__).parents[3] / "logs"


def get_selectors(num: int) -> tuple[str, str, str]:
    # returns css selectors for:
    #   chart position, title, artist
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
    date_ = start_date
    while date_ <= end_date:
        yield date_
        date_ += timedelta(days=7)


async def url_producer(dates: Iterator[date], queue1: asyncio.Queue, chart_name: str):
    for i, _date in enumerate(dates, start=1):
        tail_url: str = f"{chart_name}/{_date.strftime('%Y-%m-%d')}/"
        await queue1.put((i, _date, tail_url))
    for _ in range(15):
        await queue1.put(None)


async def clean_worker(
    chart_name: str, queue2: asyncio.Queue, num_workers: int
) -> list[Charts]:
    i: int = 0
    charts_list: list[Charts] = []
    while i < num_workers:
        chart_data: Charts | None = await queue2.get()
        if chart_data is None:
            i += 1
        else:
            chart_data.name = chart_name
            charts_list.append(chart_data)
        queue2.task_done()
    return charts_list


async def scrape_worker(
    num: int,
    counter: AsyncCounter,
    queue1: asyncio.Queue,
    queue2: asyncio.Queue,
    client: ClientSession,
):
    while True:
        item: tuple[int, date, str] | None = await queue1.get()
        if item is None:
            break
        chart_num, date_, tail_url = item
        async with client.get(tail_url) as r:
            r.raise_for_status()
            r_body: str = await r.text(encoding="utf-8")
        charts: Charts = await parse_html(r_body, date_)
        queue1.task_done()
        await counter.add()
        await queue2.put(charts)
    await queue2.put(None)


async def parse_html(r_body: str, date_: date) -> Charts:
    entries: list[Entries] = []
    tree: HTMLParser = HTMLParser(r_body)
    chart_num: int = 1
    while True:
        position_tag, title_tag, artist_tag = [
            tree.css_first(s) for s in get_selectors(chart_num)
        ]
        if position_tag and title_tag and artist_tag:
            attrs = {
                "position": int(position_tag.text(strip=True)),
                "artist": re.sub(r"(?<! )[aA]nd", " And", artist_tag.text(strip=True)),
                "title": title_tag.text(strip=True)
                .replace("RE-\nENTRY", "")
                .replace("NEW", ""),
            }

            entries.append(Entries(**attrs))

            if attrs["position"] == 100:
                break

        chart_num += 1

    chart: Charts = Charts(date=date_, entries=entries)
    return chart


async def extract(
    chart_name: str, start_date: date, end_date: date | None = None
) -> tuple[list[Charts], date]:
    if not end_date:
        end_date = date.today()
    end_date = to_saturday(end_date)

    start_time: float = time.time()
    total_charts: int = calc_num_charts(start_date, end_date)
    queue1: asyncio.Queue = asyncio.Queue(maxsize=15)
    queue2: asyncio.Queue = asyncio.Queue()
    counter: AsyncCounter = AsyncCounter(stop_at=total_charts)
    client: ClientSession = ClientSession(
        base_url="https://www.billboard.com/charts/",
        middlewares=[retry_middleware],
        connector=TCPConnector(limit=30),
    )
    async with asyncio.TaskGroup() as tg:
        dates: Iterator[date] = date_generator(start_date, end_date)
        tg.create_task(progress_report(total_charts, counter))
        tg.create_task(url_producer(dates, queue1, chart_name))
        for i in range(5):
            tg.create_task(scrape_worker(i + 1, counter, queue1, queue2, client))
        result_task = tg.create_task(clean_worker(chart_name, queue2, 5))

    await client.close()
    print(f"extract completed in {time.time() - start_time} seconds")
    return result_task.result(), end_date


# if __name__ == "__main__":
#     r = asyncio.run(extract())
#     print(r)
