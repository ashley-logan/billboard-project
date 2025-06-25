import duckdb
from datetime import date
from pathlib import Path

root_dir = Path(__file__).parent.parent
db_output_folder = root_dir / "data" / "processed_data"
db_output_folder.mkdir(parents=True, exist_ok=True)
raw_output_folder = root_dir / "data" / "raw_data"
raw_output_folder.mkdir(parents=True, exist_ok=True)

DB_PATH = db_output_folder / "chart-analytics.db"
RAW_PATH = raw_output_folder
OLDEST_RECORD_DATE: date = date(1958, 8, 4)

create_table_queries: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS songs (
    id UINT64 PRIMARY KEY,
    name TEXT,
    chart_debut DATE,
    decade TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS records (
    date DATE,
    position INTEGER,
    id_song UINT64,
    PRIMARY KEY (date, position),
    FOREIGN KEY (id_song) REFERENCES songs(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS artists (
    id UINT64 PRIMARY KEY,
    name TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS junction (
    id_song UINT64,
    id_artist UINT64,
    role TEXT,
    FOREIGN KEY (id_song) REFERENCES songs(id),
    FOREIGN KEY (id_artist) REFERENCES artists(id),
    PRIMARY KEY (id_song, id_artist, role)
    );
    
    """,
]


def date_range_to_scrape() -> tuple[date, date]:
    with duckdb.connect(DB_PATH) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS charts (
            charts.date DATE,
            charts.position INTEGER,
            charts.song TEXT,
            charts.artists TEXT,
            PRIMARY KEY (charts.date, charts.position)
            );
            """
        )

        start_date: date = con.execute(
            "SELECT IFNULL(MAX(date) + INTERVAL 7 DAY, ?) FROM charts",
            [OLDEST_RECORD_DATE],
        ).fetchone()[0]
        end_date: date = con.execute("SELECT CURRENT_DATE").fetchone()[0]
        return start_date, end_date
