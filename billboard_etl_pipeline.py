
from joining_dict_ import JoiningDict
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
    PARENT_TAG = 'ul' # parent tag that contains all relevant nodes
    CHILD_TAGS = ['span', 'h3'] # tags within parent node that contain relevant data
    PARENT_CLASS = 'o-chart-results-list-row //' # beginning of the parent class
    TEXT_IGNORE = ['NEW', 'RE-\nENTRY'] # irrelevant text that shouldn't be returned 

    def __init__(self):
        self.content = [] # returns all of the rows as lists of strings
        self.curr_row = [] # holds the data as a list of strings
        self.state = 'default'
        self.row_counter = 0

        self.STATES = {
        'default': self.handle_default,
         'in_row': self.handle_in_row
         }


    def is_parent(self, ele):
        return ele.tag == self.PARENT_TAG and self.PARENT_CLASS in ele.attrib.get('class', '')

    def is_child(self, ele):
        return ele.tag in self.CHILD_TAGS

    def handle_default(self, event, ele):
        if event == 'start' and self.is_parent(ele):
            self.state = 'in_row'
            self.row_counter += 1

    def handle_in_row(self, event, ele):
        if event == 'end' and self.is_parent(ele):
            self.state = 'default'
            assert self.curr_row
            self.content.append(self.curr_row)
            self.curr_row = []
        elif event == 'end' and self.is_child(ele):
            data = self.clean_text(ele)
            if data:
                self.curr_row.append(data)

    def on_event(self, event, ele):
        self.STATES[self.state](event, ele)
        if event == 'end':
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
        output = ''
        for i, row in enumerate(self.content):
            output += f"row {i}: {row}\n"
        return output


def to_saturday(date, scalar=1):
    # rounds the date to a Saturday
    while date.weekday() != 5:
        date = date + scalar*dt.timedelta(days=1)
    return date

def date_generator(latest_date=to_saturday(dt.datetime.today()), delta=1):
    # infintite generator for dates that takes the timedelta as a paramater
    latest_date = to_saturday(latest_date)
    curr = latest_date
    while curr >= OLDEST:
        yield curr
        curr -= dt.timedelta(weeks=delta)

def get_url(date):
    return date.strftime('%Y-%m-%d') + '/'

def offload_parser(parser, chunk, handler):
    parser.feed(chunk)
    for event, ele in events_generator(parser, handler):
        handler(event, ele)

def format_data(date, data):
    idxs = [0, 1, 2, 5]
    columns = ['position', 'song', 'artist', 'wks_on_chart']
    # cast = lambda y: 0 if y=='-' else (lambda z: int(z) if z.isnumeric() else z)(y)
    # fills '-' values with 0 and casts numceric columns to integers
    result = [
        {col: row[i]
        for col, i in zip(columns, idxs)}
        for row in data
        ]

    for record in result:
        record.update({'date': date})

    return result

    
# testing parser feed() and read_events() methods
async def html_driver(date, client):
    url = get_url(date)
    parser = etree.HTMLPullParser(events=("start", "end"))
    handler = HTMLHandler()
    
    async with client.stream('GET', url) as response:
        async for chunk in response.aiter_text():
            offload_parser(parser, chunk, handler)
            if len(handler) >= 100:
                 break
    data = handler.get_content()

    return format_data(date, data)

def events_generator(parser, handler, max_len=100):
    events = parser.read_events()
    for event, ele in events:
        if len(handler) >= max_len:
            break
        yield event, ele





async def main():
    start_time = time.time()
    BUFFER_SIZE = 800
    dates = date_generator(delta=52)
    buffer = [] 
    tasks = []
    async with httpx.AsyncClient(
        base_url="https://www.billboard.com/charts/hot-100/", timeout=15.0
        ) as client:
        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(
                    html_driver(date, client)) for date in dates
                    ]

    results = []
    for future in asyncio.as_completed(tasks):
        data = await future
        buffer.extend(data)
    
        if len(buffer) >= BUFFER_SIZE:
            # print(buffer)
            results.append(pd.DataFrame.from_records(buffer))
            buffer.clear()
            print(f"flushed buffer at {time.time() - start_time} seconds")

    if buffer:
        # print(buffer)
        results.append(pd.DataFrame.from_records(buffer))
        print(f"final flush at {time.time() - start_time} seconds")
    return results



if __name__ == '__main__':
    data = asyncio.run(main())
    print(data)


