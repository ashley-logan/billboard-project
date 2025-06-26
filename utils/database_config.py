import duckdb
from datetime import date

OLDEST_RECORD_DATE: date = date(1958, 8, 4)


def date_range_to_scrape(db_path) -> list[date]:
    with duckdb.connect(db_path) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS charts (
            date DATE,
            position INTEGER,
            song TEXT,
            artists TEXT[],
            PRIMARY KEY (date, position)
            );
            """
        )

        start_date: date = con.execute(
            "SELECT IFNULL(MAX(date) + INTERVAL 7 DAY, ?) FROM charts",
            [OLDEST_RECORD_DATE],
        ).fetchone()[0]
        end_date: date = con.execute("SELECT CURRENT_DATE").fetchone()[0]
        dates: list[date] = []
        for _date in (start_date, end_date):
            try:
                dates.append(_date.date())
            except AttributeError:
                dates.append(_date)
        return dates
