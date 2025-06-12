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
    decades: range = range(1960, 2030, 10)
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
        .select(
            [
                "id",
                "song",
                "artist",
                "power",
                "longevity",
                "earliest",
                "latest",
                "decade",
            ]
        )
        .collect()
    )


def parse_artists(song_df) -> pl.DataFrame:
    pattern1 = r"(?i)(^.*?)(?:\sfeat.?[a-z]*\s|with\s)(.*$)"
    pattern2 = r"(([^&,/+]+)+?)"

    song_df: pl.DataFrame = song_df.with_columns(
        pl.col("artist").replace(r"(?i)duet\swith", "&")
    )

    main_df: pl.DataFrame = song_df.select(
        artist=pl.col("artist").str.extract(pattern1, 1)
    ).with_columns(role=pl.lit("main"))
    feat_df: pl.DataFrame = song_df.select(
        artist=pl.col("artist").str.extract(pattern1, 2)
    ).with_columns(role=pl.lit("featured"))
    df = pl.concat([main_df, feat_df]).drop_nulls(subset=["artist"])

    df = df.select(
        pl.col("artist")
        .str.replace(r"\s[xX]\s", r"\s&\s")
        .str.strip_chars()
        .str.extract_all(pattern2),
        pl.col("role"),
    ).with_columns(pl.col("artist").list.eval(pl.element().str.strip_chars()))


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
    base_table = load()
    base_table = base_table.pipe(clean)
    song_table = create_table_song(base_table.lazy())
    # artist_table = create_table_artist(base_table.lazy())
    # junction_table = create_table_junction(base_table.lazy(), song_table, artist_table)
    print(parse_artists(song_table).head(40))


if __name__ == "__main__":
    main()
