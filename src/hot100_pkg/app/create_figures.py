import polars as pl
from hot100_pkg.utils import get_db_conn


def create_table():
    with get_db_conn() as conn:
        df = conn.execute("select * from charts").pl()
    df
