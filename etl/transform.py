from datetime import date
import polars as pl
import adbc_driver_sqlite.dbapi as sql_driver
import pyarrow
import sqlite3

pl.Config.set_tbl_cols(20)
pl.Config.set_tbl_rows(20)
con = sql_driver.connect("./etl/billboard.db")
"""
TRANFORMATION OUTLINE:
CREATE TABLE records (
id INT PRIMARY KEY
position INT
date "DATE 
song_id INT FOREIGN KEY
)

CREATE TABLE song (
id INT PRIMARY KEY
name VARCHAR
power FLOAT
longevity FLOAT
earliest DATE
latest DATE
decade TEXT
)

CREATE TABLE artist (
id INT PRIMARY KEY
name VARCHAR
)

CREATE TABLE song-artist (
(artist_id INT, song_id INT) PRIMARY KEY
)

"""


def clean(df):
    return (
        df.cast({"date": pl.Date, "position": pl.UInt8})
        .filter(pl.col("date") >= date(1960, 1, 1))
        .sort(by="date")
        .with_row_index("id")
        .select(["id", "date", "position", "song", "artist"])
    )


def load():
    return pl.read_json("./data/records05-29_23-57.json")
    rawdf: pl.DataFrame = pl.read_json("./data/records05-29_23-57.json")
    cleandf: pl.DataFrame = rawdf.pipe(clean)
    cleandf.write_database(
        table_name="records_table",
        connection=con,
        if_table_exists="fail",
        engine="adbc",
    )


def create_table_song(lf):
    decades: range = range(1970, 2030, 10)
    return (
        lf.cast({"date": pl.Date})
        .group_by(["song", "artist"])
        .agg(
            power=(1 / pl.col("position")).sum(),
            longevity=(1 / (pl.col("position").log1p())).sum(),
            weeks_on_chart=pl.len(),
            proportion_top10=((pl.col("position") <= 10).sum() / pl.len()),
            earliest=pl.min("date"),
            latest=pl.max("date"),
        )
        .with_row_index("id")
        .sort(by="earliest")
        .with_columns(
            decade=(
                pl.col("earliest")
                .dt.year()
                .cut(
                    breaks=decades,
                    labels=[f"{x - 10}s" for x in decades] + ["2020s"],
                    left_closed=True,
                )
            )
        )
        .sort(by="power", descending=True)
        .select(["id", "song", "power", "longevity", "earliest", "latest", "decade"])
        .collect()
    )


def art_expansion_expr(col):
    return (
        pl.col(col)
        .str.split(" Featuring ")
        .list.first()
        .str.split(" With ")
        .list.eval(pl.element().str.split("&").list.eval(pl.element().str.split(",")))
    )


def create_table_junction(lf, song_table) -> pl.DataFrame:
    return (
        lf.unique(subset=["song", "artist"])
        .select(song=pl.col("song"), artist=art_expansion_expr("artist"))
        .explode(["artist"])
        .explode(["artist"])
        .explode(["artist"])
        .collect()
    )


def create_table_artist(lf) -> pl.DataFrame:
    return (
        lf.select(
            artist=art_expansion_expr("artist").flatten().flatten().flatten().unique()
        )
        .with_row_index("id")
        .collect()
    )


def main():
    base_table = load()
    base_table = base_table.pipe(clean)
    song_table = create_table_song(base_table.lazy())
    artist_table = create_table_artist(base_table.lazy())
    print(artist_table.tail(20))
    # print(song_table.tail(20))


if __name__ == "__main__":
    main()
