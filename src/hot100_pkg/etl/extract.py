import asyncio
from concurrent.futures import ProcessPoolExecutor
from datetime import date, timedelta
import time
import json
import random
from pathlib import Path
from typing import Iterator
from aiohttp import ClientSession, TCPConnector
from hot100_pkg.database import Charts, Entries, write_batch
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
    # yield one day per week until end_date is reached
    while date_ <= end_date:
        yield date_
        date_ += timedelta(days=7)


async def url_producer(
    dates: Iterator[date], queue1: asyncio.Queue, chart_name: str, num_workers: int
):
    # produces chart urls for each date in dates iterator and puts them into queue1
    for i, _date in enumerate(dates, start=1):
        tail_url: str = f"{chart_name}/{_date.strftime('%Y-%m-%d')}/"
        await queue1.put((i, _date, tail_url))
    for _ in range(num_workers):
        # put a sentinel value into queue1 for each scrape worker to signal that the url producer has finished
        await queue1.put(None)


async def scrape_worker(
    num: int,
    name: str,
    counter: AsyncCounter,
    queue1: asyncio.Queue,
    queue2: asyncio.Queue,
    client: ClientSession,
    pool: ProcessPoolExecutor,
):
    # parse the html response body for each url into a Chart object and put into queue2
    while True:
        item: tuple[int, date, str] | None = await queue1.get()
        if item is None:
            # if sentinel value is received, end worker
            break
        # unstructure tuple into date and url
        chart_num, date_, tail_url = item
        # get html response body from url
        async with client.get(tail_url) as r:
            r.raise_for_status()
            r_body: str = await r.text(encoding="utf-8")
        loop = asyncio.get_running_loop()
        # offloads the parsing of html into a list of entries objects into a process pool since parsing is cpu-bounded
        entries: list[Entries] = await loop.run_in_executor(pool, parse_html, r_body)
        # create a charts object from the chart date and entries list
        chart: Charts = Charts(date=date_, name=name, entries=entries)
        # mark the current url response as parsed/done
        queue1.task_done()
        # increment the parsed charts counter
        await counter.add()
        await queue2.put(chart)
        # if 100 charts are waiting to be written to the database, block event loop until batch is written
        if queue2.full():
            write_batch(queue2)
        # sleep to avoid flooding the billboard site
        await asyncio.sleep(random.expovariate(1.0))


async def extract(
    chart_name: str, start_date: date, end_date: date | None = None
) -> tuple[int, date]:
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
    pool: ProcessPoolExecutor = ProcessPoolExecutor()
    async with asyncio.TaskGroup() as tg:
        dates: Iterator[date] = date_generator(start_date, end_date)
        tg.create_task(progress_report(total_charts, counter))
        tg.create_task(url_producer(dates, queue1, chart_name, 5))
        for i in range(5):
            tg.create_task(
                scrape_worker(i + 1, chart_name, counter, queue1, queue2, client, pool)
            )

    await client.close()
    pool.shutdown(wait=True)
    write_batch(queue2)
    queue2.join()
    num_charts: int = await counter.get()

    print(f"{num_charts} extracted in {time.time() - start_time} seconds")
    return num_charts, end_date


# if __name__ == "__main__":
#     r = asyncio.run(extract())
#     print(r)
