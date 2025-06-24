import duckdb
import polars as pl
from datetime import date
from pathlib import Path

OLDEST_RECORD_DATE = date(1958, 8, 4)
ROOT_DIR: Path = Path(__file__).parent.parent


def load(dfs: dict[str, pl.DataFrame], db_path):
    with duckdb.connect(database=str(db_path)) as con:
        for df_name, df in dfs.items():
            con.register(df_name, df)
            con.execute(f"INSERT INTO {str(df_name)[:-2]} SELECT * FROM {df_name}")
