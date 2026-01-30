from datetime import date, timedelta
import time
import random
from aiohttp import ClientHandlerType, ClientRequest, ClientResponse
import asyncio
import json
import os
from pathlib import Path
from .counter_class import AsyncCounter


OLDEST_RECORD_DATE: date = date(1958, 8, 2)


def to_saturday(date_: date, round_up: bool = False) -> date:
    delta: int = 1 if round_up else -1
    while date_.weekday() != 5:
        date_ += timedelta(days=delta)
    return date_


def load_cache(cache_path: Path) -> dict:
    with open(cache_path, "r") as f:
        data: dict = json.load(f)
    return data


def update_cache(cache_path: Path, most_recent: date, records_added: int):
    data = {}
    if os.path.isfile(cache_path):
        data = load_cache(cache_path)
    data["most_recent_chart_date"] = most_recent.isoformat()
    old_total_charts: int = data.get("total_charts") or 0
    data["total_charts"] = old_total_charts + records_added
    data["charts_added_prev_run"] = records_added
    with open(cache_path, "w") as f:
        json.dump(data, f, indent=4)


def calc_num_charts(start_date: date, end_date: date) -> int:
    delta: timedelta = end_date - start_date
    return (delta // timedelta(days=7)) + 1


async def progress_report(num_charts: int, counter: AsyncCounter):
    start_time: float = time.time()
    while counter.is_active:
        c = await counter.get()
        print(
            f"{round(time.time() - start_time, 3)}s -- {c}/{num_charts} charts extracted"
        )
        await asyncio.sleep(15)


async def retry_middleware(
    req: ClientRequest, handler: ClientHandlerType
) -> ClientResponse:
    for i in range(5):
        r: ClientResponse = await handler(req)
        if r.ok:
            return r
        backoff: float = 2**i + random.expovariate(1.0 / 0.3)
        await asyncio.sleep(backoff)
    return r
