import duckdb
import polars as pl
from datetime import date
from pathlib import Path

OLDEST_RECORD_DATE = date(1958, 8, 4)
ROOT_DIR: Path = Path(__file__).parent.parent


def load(clean_df: pl.DataFrame, db_path):
    with duckdb.connect(database=str(db_path)) as con:
        con.register("charts_df", clean_df)
        con.execute("INSERT INTO charts SELECT * FROM charts_df")
        con.execute(
            """
            CREATE TEMP TABLE new_songs AS ( 
            SELECT
                song AS song_title,
                artists AS song_artists,
                sum(1 / charts.position) AS pos_score,
                sum(101 - charts.position) AS long_score,
                sum(1 / ln(charts.position + 1)) AS overall_score,
                strftime(min(charts.date),'%B %-d, %Y') AS chart_debut
            FROM charts
            GROUP BY
            ALL
            );
            """
        )
        con.execute(
            """
            INSERT INTO songs (song_title, song_artists, pos_score, long_score, overall_score, chart_debut) SELECT * FROM new_songs 
            ON CONFLICT (song_title) DO UPDATE SET
                pos_score = pos_score + excluded.pos_score,
                long_score = long_score + excluded.long_score,
                overall_score = overall_score + excluded.overall_score,
                chart_debut = 
                    CASE
                        WHEN strptime(excluded.chart_debut, '%B %-d, %Y') < strptime(chart_debut, '%B %-d, %Y') THEN excluded.chart_debut
                        ELSE chart_debut
                    END;
            """
        )
