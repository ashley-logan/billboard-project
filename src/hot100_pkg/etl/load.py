import polars as pl
from datetime import date
from pathlib import Path
from hot100_pkg.utils import get_db_conn


def load(clean_df: pl.DataFrame):
    con = get_db_conn()
    con.register("charts_df", clean_df)
    con.execute("INSERT INTO charts SELECT * FROM charts_df")
    con.close()
