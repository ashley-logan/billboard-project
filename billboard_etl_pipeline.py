from collections import Counter
import httpx
import asyncio
import datetime as dt
import pylint
from lxml import etree
import time
import duckdb
import pandas as pd

OLDEST = dt.datetime(1958, 8, 4)


class HTMLHandler:
    PARENT_TAG = "ul"  # parent tag that contains all relevant nodes
    CHILD_TAGS = ["span", "h3"]  # tags within parent node that contain relevant data
    PARENT_CLASS = "o-chart-results-list-row //"  # beginning of the parent class
    TEXT_IGNORE = ["NEW", "RE-\nENTRY"]  # irrelevant text that shouldn't be returned

    def __init__(self):
        self.content = []  # returns all of the rows as lists of strings
        self.curr_row = []  # holds the data as a list of strings
        self.state = "default"

        self.STATES = {"default": self.handle_default, "in_row": self.handle_in_row}

    def is_parent(self, ele):
        return ele.tag == self.PARENT_TAG and self.PARENT_CLASS in ele.attrib.get(
            "class", ""
        )

    def is_child(self, ele):
        return ele.tag in self.CHILD_TAGS

    def handle_default(self, event, ele):
        if event == "start" and self.is_parent(ele):
            self.state = "in_row"

    def handle_in_row(self, event, ele):
        if event == "end" and self.is_parent(ele):
            self.state = "default"
            assert self.curr_row
            self.content.append(self.curr_row)
            self.curr_row = []
        elif event == "end" and self.is_child(ele):
            data = self.clean_text(ele)
            if data:
                self.curr_row.append(data)

    def on_event(self, event, ele):
        self.STATES[self.state](event, ele)
        if event == "end":
            ele.clear()

    def __call__(self, event, ele):
        self.on_event(event, ele)

    def clean_text(self, ele):
        if not ele.text or ele.text.strip() in self.TEXT_IGNORE:
            return None
        else:
            return ele.text.strip()

    def get_content(self):
        if self.curr_row:
            self.content.append(self.curr_row)
        return self.content

    def __len__(self):
        return len(self.content)

    def __str__(self):
        output = ""
        for i, row in enumerate(self.content):
            output += f"row {i}: {row}\n"
        return output


def to_saturday(date, scalar=1):
    # rounds the date to a Saturday
    while date.weekday() != 5:
        date = date + scalar * dt.timedelta(days=1)
    return date


def date_generator(latest_date=to_saturday(dt.datetime.today()), delta=1):
    # infintite generator for dates that takes the timedelta as a paramater
    latest_date = to_saturday(latest_date)
    curr = latest_date
    while curr >= OLDEST:
        yield curr
        curr -= dt.timedelta(weeks=delta)


def get_url(date):
    return date.strftime("%Y-%m-%d") + "/"


def events_generator(parser, handler, max_len=100):
    events = parser.read_events()
    for event, ele in events:
        if len(handler) >= max_len:
            break
        yield event, ele


def offload_parser(parser, chunk, handler, events_iter):
    parser.feed(chunk)
    for event, ele in events_iter:
        handler(event, ele)


def format_index(date, data):
    indexed_ = []
    idxs = [1, 2, 5]
    columns = ["date", "position", "song", "artist", "wks_on_chart"]
    for position, entry in enumerate(data, start=1):
        indexed_.append([date, position] + [entry[i] for i in idxs])

    formatted_indexed = [dict(zip(columns, entry)) for entry in indexed_]
    
    return formatted_indexed


def format_data(data_):
    columns = ["date", "position", "song", "artist", "wks_on_chart"]
    formatted = [dict(zip(columns, row)) for row in data_]

    return formatted


# testing parser feed() and read_events() methods
async def html_driver(date, client):
    url = get_url(date)
    parser = etree.HTMLPullParser(events=("start", "end"))
    handler = HTMLHandler()

    async with client.stream("GET", url) as response:
        async for chunk in response.aiter_text():
            events_ = events_generator(parser, handler)
            parser.feed(chunk)
            for event, ele in events_:
                handler(event, ele)
            if len(handler) >= 100:
                break
    return date, handler.get_content()


async def main():
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE IF NOT EXISTS hot100 (date DATE, position UINT8, song VARCHAR, artist VARCHAR, wks_on_chart UINT16, PRIMARY KEY (date, position))"
    )
    start_time = time.time()
    BUFFER_SIZE = 2000
    dates = date_generator(delta=4)
    buffer = []
    tasks = []
    async with httpx.AsyncClient(
        base_url="https://www.billboard.com/charts/hot-100/", timeout=15.0
    ) as client:
        async with asyncio.TaskGroup() as tg:
            tasks = [tg.create_task(html_driver(date, client)) for date in dates]

    for task in tasks:
        if task.exception():
            raise "task failed"
        date, data = task.result()
        transformed_data = format_index_index(date, data)
        # formatted_data = format_data(indexed_data)
        buffer.extend(transformed_data)
    
        if len(buffer) >= BUFFER_SIZE:
            df = pd.DataFrame.from_records(buffer)
            con.execute("INSERT INTO hot100 SELECT * FROM df")
            buffer.clear()
            print(f"flushed buffer at {time.time() - start_time} seconds")

    if buffer:
        df = pd.DataFrame.from_records(buffer)
        con.execute("INSERT INTO hot100 SELECT * FROM df")
        print(f"final flush at {time.time() - start_time} seconds")

    print(con.execute("SELECT COUNT(*) FROM hot100").fetchone())


if __name__ == "__main__":
    # asyncio.run(main())
    con.close()
