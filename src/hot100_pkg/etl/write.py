from hot100_pkg.database import Charts, Base
from hot100_pkg.configs import DB_URI
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
import asyncio


engine = create_async_engine(DB_URI)

async_session: async_sessionmaker[AsyncSession] = async_sessionmaker(engine)


def write_db(charts: list[Charts]):
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        s.add_all(sorted(charts, key=lambda x: x.date))
        s.commit()


def write_batch(queue2: Queue):
    # accumulate each chart object into a list and write to the database
    batch: list[Charts] = []
    while True:
        try:
            chart = queue2.get_nowait()
            batch.append(chart)
            queue2.task_done()
        except QueueEmpty:
            break
    # write charts to the database
    write_db(batch)


async def async_stream_from_queue(queue: asyncio.Queue):
    while True:
        item = await queue.get()
        if item is None:  # sentinel for shutdown
            break
        yield item
        queue.task_done()


async def async_writer(q: asyncio.Queue):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    batch: list[Charts] = []
    while True:
        chart = await q.get()
        if chart is None:
            break
        batch.append(chart)
        if len(batch) >= 100:
            async with async_session() as session:
                async with session.begin():
                    session.add_all(batch)
            batch.clear()
    if batch:
        async with async_session() as session:
            async with session.begin():
                session.add_all(batch)
    batch.clear()


async def consumer(queue: asyncio.Queue, session):
    batch: list[Charts] = []
    async for item in async_stream_from_queue(queue):
        batch.append(item)

        if len(batch) >= 1000:
            await session.execute(insert(MyTable), batch)
            await session.commit()
            batch.clear()

    if batch:
        await session.execute(insert(MyTable), batch)
        await session.commit()
