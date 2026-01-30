from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from .models import Charts, Base
from pathlib import Path

DB_PATH: Path = Path(__file__).resolve().parents[3] / "data" / "dev.db"


def write_db(charts: list[Charts]) -> int:
    engine = create_engine(f"sqlite:///{DB_PATH.resolve().as_posix()}")
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        s.add_all(sorted(charts, key=lambda x: x.date))
        s.commit()
    return len(charts)
