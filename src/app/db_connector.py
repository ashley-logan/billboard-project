import polars as pl
from utils import get_db_conn

con = get_db_conn()
charts_df = con.execute(
"""
SELECT ns.id, c.*
FROM chart c
JOIN new_songs ns ON c.song = ns.title
"""
).pl()

def filter_charts(limit: int=10, by_decade: bool=True, rankings: list=["by_pos", "by_longevity", "average"]):
    return (
        charts_df.group_by("id")
        .agg(
            pos_score=pl.sum(1/pl.col("position"),
            long_score=pl.sum(101-pl.col("position")),
            avg_score=pl.sum(1/pl.col("position").logp())
        )
        .with_columns(
        position_score=sum()
        longevity_rank=,
        average_rank,
    )
    )