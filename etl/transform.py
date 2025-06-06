from datetime import date
import polars as pl
import adbc_driver_sqlite.dbapi as sql_driver
import pyarrow
import sqlite3

pl.Config.set_tbl_cols(20)
pl.Config.set_tbl_rows(20)
con  = sql_driver.connect("./etl/billboard.db")
"""
TRANFORMATION OUTLINE:
convert ["date"] to dtype pl.Date
drop ["wks_on"chart]
add ranged indexed column named "record_id"
groupby ["song"]
    create calculated column from expression (1/["position"]).sum() names "popularity_score"
    created caluclated column from value counts named "wks_on_chart"
    create range index column named "song_id"
    create calculated column from expr ["date"].min() named "earliest_record_date"
    create calculated column from expr ["date"].max() named "lastest_record_date"
    songdf = df.group_by
    (pl.len()+1 - pl.col("rank") / pl.len()+1).mul()
["peak_score", "longevity_score"].rank(descending=True)

"""

def clean(df):
    return df.cast({"date": pl.Date, "position": pl.UInt8}).filter(
        pl.col("date") >= date(1960, 1, 1)).sort(by="date").with_row_index("id").select([
            "id",
            "date",
            "position",
            "song",
            "artist"
        ])

def load():
    rawdf: pl.DataFrame = pl.read_json("./data/records05-29_23-57.json")
    cleandf: pl.DataFrame = rawdf.pipe(clean)
    cleandf.write_database(
        table_name="records_table",
        connection=con,
        if_table_exists="fail",
        engine="adbc"
    )

def init_tables():
    with con.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE records_table (
            id INTEGER PRIMARY KEY
            
            )
            """
        )


if __name__ == "__main__":
    con.close()


