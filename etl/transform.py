import json
import polars as pl
from numpy import log
from pathlib import Path
import re

pl.Config.set_tbl_cols(20)

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


# def create_song_table(df):
#     songdf = (
#         df.group_by("song")
#         .agg(
#             (1 / pl.col("position")).sum().alias("peak_popularity_score"),
#             (1 / (log(pl.col("position")))).sum().alias()
#             pl.len().alias("wks_on_chart"),
#             pl.col("artist").mode().first(),
#         )
#         .select(
#             pl.col("peak_popularity_score").rank(descending=True).name.suffix("_rank"),
#             pl.col("staying_power_score").rank(descending=True).name.suffix("_rank"),
#         )
#         .with_columns(
#             ((pl.len() + 1 - pl.col("peak_popularity_score_rank")) / (pl.len() + 1))
#             .mul(100)
#             .alias("peak_pct")
#         )
#     )
# return songdf
def get_zscore(col):
    return (pl.col(col) - pl.col(col).std()) / pl.col(col).mean()


def join_on(df1, df2, col):
    return df1.join(df2, on=col, how="inner")


def avg_pct(df):
    power_pct = pl.col("power_score").rank(method="ordinal") / pl.len()
    longevity_pct = pl.col("longevity_score").rank(method="ordinal") / pl.len()
    return (100 * (power_pct + longevity_pct) / 2).round(2)


def get_pct(df):
    q = (
        df.lazy()
        .with_columns(
            track_id=pl.arange(0, pl.len()).sort(descending=True),
            average_percentile=df.pipe(avg_pct),
        )
        .sort(by="average_percentile", descending=True)
        .select(
            [
                "track_id",
                "song",
                "artists",
                "features",
                "power_score",
                "longevity_score",
                "average_percentile",
                "proportion_top10",
                "weeks_on_chart",
            ]
        )
    )
    songdf = q.collect()
    return songdf


def get_query(df):
    df = (
        df.lazy()
        .group_by(["song", "artists", "features"])
        .agg(
            (1 / pl.col("position")).sum().alias("power_score"),
            (1 / (log(pl.col("position").mul(10)))).sum().alias("longevity_score"),
            pl.len().alias("weeks_on_chart"),
            ((pl.col("position") <= 10).sum() / pl.len())
            .round(4)
            .alias("proportion_top10"),
        )
    ).collect()
    return df


def split_col(df):
    q_plan = (
        df.lazy()
        .with_columns(
            date=pl.col("date").cast(pl.Date),
            position=pl.col("position").cast(pl.UInt8),
            record_id=pl.arange(0, pl.len()).sort(descending=True),
            artists=pl.col("artist").str.split("Featuring").list.first().str.split("&"),
            features=pl.col("artist")
            .str.split("Featuring")
            .list.get(index=1, null_on_oob=True)
            .str.split("&"),
        )
        .select(
            [
                "record_id",
                "date",
                "position",
                "song",
                "artists",
                "features",
            ]
        )
    )

    return q_plan.collect()


def transform():
    filepath = "./data/records05-29_23-57.json"
    df: pl.DataFrame = pl.read_json(filepath)
    maindf = df.pipe(split_col)
    trackdf = maindf.pipe(get_query)
    trackdf = trackdf.pipe(get_pct)
    maindf = maindf.pipe(join_on, trackdf.select(["track_id", "song"]), "song").select(
        ["record_id", "date", "position", "track_id", "song", "artists", "features"]
    )
    print(maindf.head())
    print(trackdf.head())


if __name__ == "__main__":
    transform()
