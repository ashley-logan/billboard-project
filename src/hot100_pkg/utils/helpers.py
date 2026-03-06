from datetime import date, timedelta
import time
import random
from aiohttp import ClientHandlerType, ClientRequest, ClientResponse
import asyncio
from pathlib import Path
from .counter_class import AsyncCounter


def calc_num_charts(start_date: date, end_date: date) -> int:
    delta: timedelta = end_date - start_date
    return (delta // timedelta(days=7)) + 1


async def progress_report(num_charts: int, counter: AsyncCounter):
    start_time: float = time.time()
    while counter.is_active:
        count: int = await counter.get()
        print(
            f"{round(time.time() - start_time, 3)}s -- {count}/{num_charts} charts extracted"
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
