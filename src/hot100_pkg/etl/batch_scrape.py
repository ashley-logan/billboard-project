from datetime import date, timedelta
from httpx import AsyncClient
import asyncio

OLDEST_RECORD_DATE: date = date(1958, 8, 4)

CSS_SELECTORS = {
    "title": """
    div.o-chart-results-list-row-container:nth-child(1) > 
    ul:nth-child(1) > li:nth-child(4) > ul:nth-child(1) > 
    li:nth-child(1) > h3:nth-child(1)
    """,
    "artist": """
    div.o-chart-results-list-row-container:nth-child(1) > 
    ul:nth-child(1) > li:nth-child(4) > ul:nth-child(1) > 
    li:nth-child(1) > span:nth-child(2)
    """,
    "position": """
    """,
}


def get_selector(num: int) -> tuple[str, str]:
    return (
        f"""
        div.o-chart-results-list-row-container:nth-child({num}) > 
        ul:nth-child(1) > li:nth-child(4) > ul:nth-child(1) > 
        li:nth-child(1) > h3:nth-child(1)""",
        f"""
        div.o-chart-results-list-row-container:nth-child({num}) > 
        ul:nth-child(1) > li:nth-child(4) > ul:nth-child(1) > 
        li:nth-child(1) > span:nth-child(2)""",
    )


def into_saturday(day: date) -> date:
    while day.weekday() != 5:
        day -= timedelta(days=1)
    return day


def batch_extract():
    start: date = OLDEST_RECORD_DATE
    end: date = into_saturday(date.today())


"""
pipline chain:
    produce url -> put url into queue1 -> retrieve url -> make request
    -> put response data into queue2 -> retrieve data -> parse html
div.o-chart-results-list-row-container:nth-child(1) > ul:nth-child(1) > li:nth-child(4) > ul:nth-child(1) > li:nth-child(1) > h3:nth-child(1)
"""
