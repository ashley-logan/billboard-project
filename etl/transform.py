from datetime import date
import polars as pl
import adbc_driver_sqlite.dbapi as sql_driver
import pyarrow
import sqlite3

pl.Config.set_tbl_cols(20)
pl.Config.set_tbl_rows(100)
# con = sql_driver.connect("./etl/billboard.db")
"""
TRANFORMATION OUTLINE:
CREATE TABLE records (
id INT PRIMARY KEY
date VARCHAR (ISO FORMAT) 
position INT
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


def load():
    return pl.read_json(
        source="./data/records06-14_15-34.json",
        schema={
            "date": pl.Date,
            "position": pl.UInt8,
            "song": pl.str,
            "artist": pl.str,
        },
    ).lazy()


def clean(lf):
    return (
        lf.sort(by="date")
        .with_row_index("id")
        .select(["id", "date", "position", "song", "artist"])
    )


def create_table_song(lf):
    # score_calcs: dict =  {
    #     "pos_weighted": (1 / pl.col("position")).sum,
    #     "longevity_weighted": (101 - pl.col("position")).truediv(100).sum,
    #     "unweighted": (pl.lit(100).log1p() - pl.col("position").log1p()).sum,
    # }
    decade_cuts: range = range(1970, 2030, 10)
    decade_labels = [f"{decade}s" for decade in range(1960, 2030, 10)]
    return (
        lf.cast({"date": pl.Date})
        .group_by(["song", "artist"])
        .agg(
            # position_score=score_calcs["pos_weighted"](),
            # longevity_score=score_calcs["longevity_weighted"](),
            # overall_score=score_calcs["unweighted"](),
            chart_debut=pl.min("date"),
            latest_appearance=pl.max("date"),
        )
        .with_row_index("id")
        .sort(by="chart_debut")
        .with_columns(
            decade=(
                pl.col("chart_debut")
                .dt.year()
                .cut(breaks=decade_cuts, labels=decade_labels, left_closed=True)
            )
        )
        .sort(by="id", descending=True)
        .select(
            [
                "id",
                "song",
                "artist",
                "chart_debut",
                "latest_appearance",
                "decade",
            ]
        )
    )


def create_table_junction(lf, song_table, artist_table) -> pl.DataFrame:
    pass
    return (
        lf.unique(subset=["song", "artist"])
        .join(song_table.lazy(), on=["song", "artist"], how="inner")
        .select(pl.col("id").alias("song_id"), pl.col("song"), pl.col("artist"))
        .explode(["artist"])
        .explode(["artist"])
        .join(artist_table.lazy(), on="artist", how="inner")
        .rename({"id": "artist_id"})
        .select(["song_id", "song", "artist_id", "artist"])
        .collect()
    )


def create_table_artist(lf) -> pl.DataFrame:
    pass
    return (
        lf.select(artist=art_expansion_expr("artist").flatten().flatten().unique())
        .with_row_index("id")
        .collect()
    )


def main():
    base_table = load().pipe(clean)
    song_table = base_table.pipe(create_table_song).collect()
    artist_table = base_table.pipe()


if __name__ == "__main__":
    main()
