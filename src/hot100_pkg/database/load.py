from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from .models import Charts, Base
from pathlib import Path
from asyncio import Queue, QueueEmpty

DB_PATH: Path = Path(__file__).resolve().parents[3] / "data" / "dev.db"


def write_db(charts: list[Charts]):
    engine = create_engine(f"sqlite:///{DB_PATH.resolve().as_posix()}")
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
