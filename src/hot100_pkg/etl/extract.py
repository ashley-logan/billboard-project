import asyncio
from datetime import date, timedelta
import time
import json
import random
import re
from pathlib import Path
from typing import Iterator
from aiohttp import ClientSession, TCPConnector
from selectolax.parser import HTMLParser, Node
from hot100_pkg.database import Charts, Entries, write_db
from hot100_pkg.utils import (
    to_saturday,
    AsyncCounter,
    calc_num_charts,
    progress_report,
    retry_middleware,
)
from .parser import parse_html

LOG_DIR = Path(__file__).parents[3] / "logs"


def date_generator(start_date: date, end_date: date) -> Iterator[date]:
    # infintite generator for dates incremented by one week
    date_ = start_date
    while date_ <= end_date:
        yield date_
        date_ += timedelta(days=7)


async def url_producer(
    dates: Iterator[date], queue1: asyncio.Queue, chart_name: str, num_workers: int
):
    for i, _date in enumerate(dates, start=1):
        tail_url: str = f"{chart_name}/{_date.strftime('%Y-%m-%d')}/"
        await queue1.put((i, _date, tail_url))
    for _ in range(num_workers):
        await queue1.put(None)


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
        loop = asyncio.get_running_loop()
        tree = await loop.run_in_executor(None, HTMLParser, r_body)
        entries: list[Entries] = await parse_html(tree)
        chart: Charts = Charts(date=date_, entries=entries)
        queue1.task_done()
        await counter.add()
        await queue2.put(chart)
        if queue2.full():
            await write_batch(queue2)
        await asyncio.sleep(random.expovariate(1.0))


async def write_batch(queue2: asyncio.Queue):
    batch: list[Charts] = []
    while True:
        try:
            chart = queue2.get_nowait()
            batch.append(chart)
            queue2.task_done()
        except asyncio.QueueEmpty:
            break
    write_db(batch)


async def extract(
    chart_name: str, start_date: date, end_date: date | None = None
) -> date:
    if not end_date:
        end_date: date = date.today()
    end_date: date = to_saturday(end_date)

    start_time: float = time.time()
    total_charts: int = calc_num_charts(start_date, end_date)
    queue1: asyncio.Queue = asyncio.Queue(maxsize=15)
    queue2: asyncio.Queue = asyncio.Queue(maxsize=100)
    counter: AsyncCounter = AsyncCounter(stop_at=total_charts)
    client: ClientSession = ClientSession(
        base_url="https://www.billboard.com/charts/",
        middlewares=[retry_middleware],
        connector=TCPConnector(limit=30),
    )
    async with asyncio.TaskGroup() as tg:
        dates: Iterator[date] = date_generator(start_date, end_date)
        tg.create_task(progress_report(total_charts, counter))
        tg.create_task(url_producer(dates, queue1, chart_name, 5))
        for i in range(5):
            tg.create_task(scrape_worker(i + 1, counter, queue1, queue2, client))

    await client.close()
    await write_batch(queue2)
    queue2.join()

    print(f"extract completed in {time.time() - start_time} seconds")
    return end_date


# if __name__ == "__main__":
#     r = asyncio.run(extract())
#     print(r)
