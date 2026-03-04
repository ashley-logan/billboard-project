import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, select, func, Row
from sqlalchemy.orm import Session
from .models import Charts, Base
from asyncio import Queue, QueueEmpty
from hot100_pkg.utils import date_generator, to_saturday


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


def get_db_tables() -> list[str]:
    with Session(engine) as s:
        inspector = inspect(s.bind)
        tables = inspector.get_table_names()
    return tables


# def read_newest() -> DateTime | None:
#     if len(get_db_tables()) == 0:
#         return None
#     with Session(engine) as s:
#         row: Row | None = s.execute(select(func.max(Charts.date))).first()
#     if row is None:
#         return None
#     return row["date"]
