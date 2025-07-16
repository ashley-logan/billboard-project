import polars as pl
from hot100_pkg.utils import get_db_conn, get_curr_date, OLDEST_RECORD_DATE

CURR_DATE = get_curr_date()
con = get_db_conn()
charts_df = con.execute(
    """
SELECT ns.id, c.*
FROM charts c
JOIN new_songs ns ON c.song = ns.title
"""
).pl()

con.close()


def filter_charts(
    limit: int = 10, start_date=OLDEST_RECORD_DATE, end_date=CURR_DATE
) -> pl.DataFrame:
    return (
        charts_df.filter(pl.col("date") >= start_date & pl.col("date") <= end_date)
        .group_by("id")
        .agg(
            pos_score=pl.sum(1 / pl.col("position")),
            long_score=pl.sum(101 - pl.col("position")),
            avg_score=pl.sum(1 / pl.col("position").logp()),
        )
        .with_columns(
            rank_bypos=pl.col("pos_score").rank("dense", descending=True),
            rank_bylong=pl.col("long_score").rank("dense", descending=True),
            rank_overall=pl.col("avg_score").rank("dense", descending=True),
        )
        .sort("rank_overall")
        .limit(n=limit)
    )
