from datetime import date, timedelta
from typing import Iterator, Callable, Optional


def to_saturday(date_: date, round_up: bool = False) -> date:
    # will return the nearest saturday to the date_ arg
    delta: int = 1 if round_up else -1
    # return the previous saturday by default unless round_up=True then returns the next saturday
    while date_.weekday() != 5:
        # 5 is saturday
        date_ += timedelta(days=delta)
    return date_


def date_generator(
    start_date: date,
    end_date: date,
    cond: Optional[Callable[[date], bool]] = None,
) -> Iterator[date]:
    # infintite generator for dates incremented by one week
    date_ = to_saturday(start_date)
    end_date = to_saturday(end_date)
    # yield one day per week until end_date is reached
    while date_ <= end_date:
        if cond is None or cond(date_):
            # if condition is not passed then always yield, otherwise check against condition
            yield date_
        date_ += timedelta(days=7)
