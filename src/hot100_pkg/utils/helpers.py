from datetime import date, timedelta
import json
import os
from pathlib import Path


OLDEST_RECORD_DATE: date = date(1958, 8, 4)


def to_saturday(date_: date, round_up: bool = False) -> date:
    delta: int = 1 if round_up else -1
    while date_.weekday() != 5:
        date_ += timedelta(days=delta)
    return date_


def load_cache(cache_path: Path) -> dict:
    with open(cache_path, "r") as f:
        data: dict = json.load(f)
    return data


def update_cache(cache_path: Path, most_recent: date, records_added: int):
    data = {}
    if os.path.isfile(cache_path):
        data = load_cache(cache_path)
    data["most_recent_chart_date"] = most_recent.isoformat()
    old_total_charts: int = data.get("total_charts") or 0
    data["total_charts"] = old_total_charts + records_added
    data["charts_added_prev_run"] = records_added
    with open(cache_path, "w") as f:
        json.dump(data, f, indent=4)
