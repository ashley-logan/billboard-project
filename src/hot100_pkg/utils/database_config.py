import duckdb
from pathlib import Path
from datetime import datetime, date

OLDEST_RECORD_DATE: date = date(1958, 8, 4)
root_dir = Path(__file__).parent.parent.parent.parent
relative_db_path = root_dir / "data" / "processed_data" / "chart-analytics.db"
DB_PATH = relative_db_path.resolve()
relative_raw_path = root_dir / "data" / "raw_data"
RAW_PATH = relative_raw_path.resolve()


def date_range_to_scrape() -> list[date]:
    with duckdb.connect(DB_PATH) as con:
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


def get_curr_date():
    return datetime.now().date()
