import datetime as dt

weekday_dict: dict[str, int] = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


def round_date(func):
    def wrapper(*args, **kwargs):
        dates: tuple = (
            kwargs.get("start_date") if "start_date" in kwargs else args[0],
            kwargs.get("end_date") if "end_date" in kwargs else args[1],
        )

        for date in dates:
            if not isinstance(date, dt.date):
                raise TypeError(f"Expected date object, got {type(date).__name__}")
        if dt.date.today().weekday() in [
            weekday_dict["sunday"],
            weekday_dict["monday"],
        ]:
            scalar: int = -1
        else:
            scalar: int = 1
        for name, date in zip(["start", "end"], dates):
            while date.weekday() != weekday_dict["saturday"]:
                date += dt.timedelta(days=scalar)
            kwargs[f"{name}_date"] = date
        if len(args) > 2:
            kwargs["delta"] = args[2]
        return func(**kwargs)

    return wrapper
