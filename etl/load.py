import duckdb
import polars as pl
from datetime import date
from pathlib import Path

OLDEST_RECORD_DATE = date(1958, 8, 4)
ROOT_DIR: Path = Path(__file__).parent.parent


def load(clean_df: pl.DataFrame, db_path):
    with duckdb.connect(database=str(db_path)) as con:
        con.register("charts_df", clean_df)
        con.execute("INSERT INTO charts SELECT * FROM charts_df")
