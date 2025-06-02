import json
import polars as pl
from numpy import log
from pathlib import Path
import re

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


# def calc_pct(df):
#     pct_expr = (pl.len() + 1 - pl.col("rank") / pl.len() + 1).mul(100)
#     df.with_columns(

#         pl.col("peak_score")
#         .rank(descending=True)
#         .(pl.len+1 - pl.col("peak_score").rank(descending=True)) / pl.len()+1)
#         .mull(100)
#     )


def create_song_table(df):
    songdf = (
        df.group_by("song")
        .agg(
            (1 / pl.col("position")).sum().alias("peak_popularity_score"),
            (1 / (log(10 * pl.col("position")))).sum().alias("staying_power_score"),
            pl.len().alias("wks_on_chart"),
            pl.col("artist").mode().first(),
        )
        .select(
            pl.col("peak_popularity_score").rank(descending=True).name.suffix("_rank"),
            pl.col("staying_power_score").rank(descending=True).name.suffix("_rank"),
        )
        .with_columns(
            ((pl.len() + 1 - pl.col("peak_popularity_score_rank")) / (pl.len() + 1))
            .mul(100)
            .alias("peak_pct")
        )
    )
    return songdf


def get_query(df):
    q = (
        df.lazy()
        .group_by(pl.col("song"))
        .agg(
            (1 / pl.col("position")).sum().alias("popularity_score"),
            pl.len().alias("wks_on_chart"),
        )
    )


def split_col(df):
    feature_col = pl.col("artist").str.split("Featuring")
    artists_col = feature_col.list.first().str.split("&")
    df = df.with_columns(
        date=pl.col("date").cast(pl.Date),
        position=pl.col("position").cast(pl.UInt8),
        record_id=pl.arange(0, pl.len()).sort(descending=True),
        artist1=artists_col.list.first(),
        artist2=artists_col.list.get(index=1, null_on_oob=True),
        artist3=artists_col.list.get(index=2, null_on_oob=True),
        feature=feature_col.list.get(index=1, null_on_oob=True),
    ).select(
        [
            "record_id",
            "date",
            "position",
            "song",
            "artist1",
            "artist2",
            "artist3",
            "feature",
        ]
    )

    return df


def clean(df):
    df = df.with_columns(
        pl.col("date").cast(pl.Date),
        pl.arange(0, pl.len()).sort(descending=True).alias("record_id"),
    )
    df = df.pipe(split_col)
    return df


def transform():
    filepath = "./data/records05-29_23-57.json"
    df: pl.DataFrame = pl.read_json(filepath)
    df = df.pipe(split_col)
    print(df)


if __name__ == "__main__":
    transform()
