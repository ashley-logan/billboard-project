from datetime import date
from dataclasses import dataclass


@dataclass
class CHART_INFO:
    name: str
    length: int
    alt_names: list[str]


# Date of the first billboard hot100 charts
OLDEST_CHART_DATE: date = date(1958, 8, 2)

CHARTS: list[CHART_INFO] = [
    CHART_INFO("hot-100", 100, []),
    CHART_INFO("billboard-200", 200, []),
    CHART_INFO("artist-100", 100, []),
    CHART_INFO("streaming-songs", 50, []),
    CHART_INFO("radio-songs", 50, []),
    CHART_INFO("digital-song-sales", 25, ["song-sales", "digital-sales"]),
    CHART_INFO("top-album-sales", 50, ["album-sales"]),
    CHART_INFO("top-streaming-albums", 50, ["streaming-albums"]),
    CHART_INFO("emerging-artists", 50, []),
]
