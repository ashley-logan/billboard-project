from datetime import date
from pathlib import Path

# Date of the first billboard hot100 charts
OLDEST_CHART_DATE: date = date(1958, 8, 2)
# Package root
ROOT: Path = Path.cwd().parents[2]
