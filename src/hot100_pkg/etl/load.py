from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from .database import Charts, Base
from pathlib import Path


def write_db(charts: list[Charts]):
    BASE_DIR: Path = Path(__file__).resolve().parent.parent
    DB_PATH: Path = BASE_DIR / "data" / "dev.db"

    engine = create_engine(f"sqlite:///{DB_PATH}")  # file-based
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        s.bulk_save_objects(charts)
