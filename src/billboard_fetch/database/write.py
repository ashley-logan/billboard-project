from .models import Chart
from billboard_fetch.configs import DB_URI
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
import asyncio


engine = create_async_engine(DB_URI)

async_session: async_sessionmaker[AsyncSession] = async_sessionmaker(engine)


async def async_add_batch(batch: list[Chart]):
    batch.sort(key=lambda x: x.date)  # sort charts by date ascending
    async with async_session() as session:
        async with session.begin():
            session.add_all(batch)
    batch.clear()  # clear buffer


async def async_writer(queue2: asyncio.Queue, num_producers: int):
    batch: list[Chart] = []  # buffer

    async def async_stream():
        # defines streaming behavior from the captured queue
        num_sentinels: int = 0

        while (
            num_sentinels < num_producers
        ):  # while there is at least one active producer
            item = await queue2.get()
            try:
                if item is None:
                    num_sentinels += 1
                    continue
                yield item
            finally:
                queue2.task_done()

    async for chart in async_stream():
        batch.append(chart)
        if len(batch) >= 100:
            # if 100 charts in buffer write them to database
            await async_add_batch(batch)

    if batch:
        await async_add_batch(batch)

    await engine.dispose()  # dispose db connection
