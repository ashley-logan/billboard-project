from .models import Charts, Base
from app.configs import DB_URI
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
import asyncio


engine = create_async_engine(DB_URI)

async_session: async_sessionmaker[AsyncSession] = async_sessionmaker(engine)


async def async_stream_from_queue(queue: asyncio.Queue):
    while True:
        item = await queue.get()
        if item is None:  # sentinel for shutdown
            break
        yield item
        queue.task_done()


async def async_writer(q: asyncio.Queue):
    async with engine.begin() as conn:
        # create all tables as defined in models.py
        await conn.run_sync(Base.metadata.create_all)
    batch: list[Charts] = []  # buffer
    while True:
        chart = await q.get()
        q.task_done()
        if chart is None:
            # None value signifies end of stream; break loop
            break
        batch.append(chart)
        if len(batch) >= 100:
            # if 100 charts in buffer write them to database
            async with async_session() as session:
                async with session.begin():
                    session.add_all(sorted(batch, key=lambda x: x.date))
            batch.clear()  # clear buffer
    if batch:
        # after stream ends write all buffered charts
        async with async_session() as session:
            async with session.begin():
                session.add_all(sorted(batch, key=lambda x: x.date))
    batch.clear()  # clear buffer
