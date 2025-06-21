import duckdb
import datetime as dt
from pathlib import Path

OLDEST_RECORD_DATE = dt.date(1958, 8, 4)

root_dir = Path(__file__).parent.parent
output_folder = root_dir / "data" / "processed_data"
output_folder.mkdir(parents=True, exist_ok=True)
duckdb_path = output_folder / "chart-analytics.duckdb"

con = duckdb.connect(str(duckdb_path), read_only=False)


start_date = con.sql(
    "SELECT COALESCE(MAX(date) + 7, {OLDEST_RECORD_DATE}) FROM records;"
)
end_date = con.sql("SELECT CURRENT_DATE")
