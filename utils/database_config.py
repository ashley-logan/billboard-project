import duckdb
from datetime import date

OLDEST_RECORD_DATE: date = date(2005, 8, 4)


def date_range_to_scrape(db_path) -> tuple[date, date]:
    with duckdb.connect(db_path) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS charts (
            date DATE,
            position INTEGER,
            song TEXT,
            artists TEXT,
            PRIMARY KEY (date, position)
            );

            CREATE sequence if not exists id_seq start 1;

            CREATE TABLE IF NOT EXISTS songs (
            id integer PRIMARY KEY DEFAULT nextval('id_seq'),
            song_title TEXT UNIQUE,
            song_artists VARCHAR[],
            chart_debut TEXT,
            pos_score FLOAT,
            long_score FLOAT,
            overall_score FLOAT,
            );
            """
        )

        start_date: date = con.execute(
            "SELECT IFNULL(MAX(date) + INTERVAL 7 DAY, ?) FROM charts",
            [OLDEST_RECORD_DATE],
        ).fetchone()[0]
        end_date: date = con.execute(
            "SELECT CURRENT_DATE - INTERVAL 10 YEAR"
        ).fetchone()[0]
        return start_date.date(), end_date.date()
