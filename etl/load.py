import duckdb
import polars as pl
import datetime as dt
from pathlib import Path

OLDEST_RECORD_DATE = dt.date(1958, 8, 4)
ROOT_DIR: Path = Path(__file__).parent.parent


def get_extract_config():
    db_path = ROOT_DIR / "data/processed_data/chart-analytics.db"
    with duckdb.connect(database=str(db_path)) as con:
        exists: bool = con.sql(
            "SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_name = 'records'"
        ).fetchone()[0]
        print(exists)
        if exists:
            start_date: dt.date = con.sql(
                "SELECT MAX(date) + INTERVAL 7 DAY FROM records"
            ).scalar()
        else:
            start_date: dt.date = OLDEST_RECORD_DATE
        end_date: dt.date = con.sql("SELECT CURRENT_DATE").fetchone()[0]

    filename = f"raw-data_{dt.date.today().strftime('%m-%d')}.parquet"
    extract_path = ROOT_DIR / "data/raw_data" / filename

    return (
        start_date,
        end_date,
    ), extract_path


def ingest(dfs: dict[str, pl.DataFrame]):
    db_path = ROOT_DIR / "data/processed_data/chart-analytics.db"
    with duckdb.connect(database=str(db_path)) as con:
        for df_name in dfs:
            con.register(df_name, dfs[df_name])
            con.execute(
                f"CREATE TABLE IF NOT EXISTS {df_name[:-2]} AS SELECT * FROM {df_name}"
            )
