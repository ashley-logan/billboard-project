import httpx
import asyncio
import datetime as dt
import pylint
import time
import duckdb
import pandas as pd
import logging
import fastparquet
from lxml import etree
from typing import Any, Generator
from handler_class import HTMLHandler


OLDEST = dt.datetime(1958, 8, 4)
# FORMAT = "%(levelname)s | at %(second).2s | %(message)s| %(variable)s"
# logging.basicConfig(
#     level=logging.DEBUG,
#     filename="pipline_logging.log", 
#     format=FORMAT,
#     filemode="w",
#     force=True
# )


class HTMLTarget(HTMLHandler):
    PARENT_TAGS = ["ul"]  # parent tag that contains all relevant nodes
    CHILD_TAGS = ["span", "h3"]  # tags within parent node that contain relevant data
    PARENT_CLASSES = ["o-chart-results-list-row //"]  # beginning of the parent class
    TEXT_IGNORE = ["NEW", "RE-\nENTRY"]



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


def events_generator(parser, handler, max_len=100) -> Generator[tuple[str, etree.Element], None, None]:
    events = parser.read_events()
    for event, ele in events:
        if len(handler) >= max_len:
            break
        yield event, ele



async def clean_data(input_queue) -> list[dict[str, Any]]:
    output = []
    while True:
        raw_data = await input_queue.get()
        # logging.debug("raw: ", extra={"second": None, "variable": raw_data})
        if raw_data is None:
            break
        clean_data = await clean(raw_data)
        # logging.debug("clean: ", extra={"second": None, "variable": clean_data})
        output.extend(clean_data)
    return output
    

async def clean(page) -> list[dict[str, Any]]:
    idxs = [1, 2, 5]
    col_names = ["date", "position", "song", "artist", "wks_on_chart"]
    date, raw_data = page
    clean_data = []
    for position, entry in enumerate(raw_data, start=1):
        try:
            row_ = [date, position] + [entry[i] for i in idxs]
            row_ = {col: data for col, data in zip(col_names, row_)}
            clean_data.append(row_)
        except:
            print(f"EXCEPTION: {entry}")

    return clean_data

    

async def scrape_data(t, generator, output_queue):
    async with httpx.AsyncClient(
        base_url="https://www.billboard.com/charts/hot-100/", timeout=60.0
    ) as asyncClient:

        # logging.info("created httpx connection client", extra={"second": time.time() - t, "variable": None})
        async with asyncio.TaskGroup() as tg:
            for date in generator:
                config = {
                    "client": asyncClient,
                    "parser": etree.HTMLPullParser(events=("start", "end")),
                    "handler": HTMLTarget(),
                    "output_queue": output_queue
                }
                tg.create_task(html_driver(t, date, **config))

    await output_queue.put(None)


# testing parser feed() and read_events() methods
async def html_driver(t, date, client, parser, handler, output_queue):
    url = get_url(date)
    await asyncio.sleep(0.2)
    async with client.stream("GET", url) as response:
        async for chunk in response.aiter_text():
            events_ = events_generator(parser, handler)
            parser.feed(chunk)
            for event, ele in events_:
                handler(event, ele)
            if len(handler) >= 100:
                break
    content = handler.get_content()
    print(content)
    await output_queue.put((date, content))

# async def batch_insert(input_queue, output_collection):
#     documents = []
#     BATCH_SIZE = 20
#     while True:
#         doc: List = await input_queue.get()
#         if doc is None:
#             break
#         documents.extend(doc)
#         if len(documents) >= BATCH_SIZE:
#             await output_collection.insert_many(documents)
#             documents = []
#     if documents:
#         await output_collection.insert_many(documents)

def transform_data(df):
    calc_popularity = lambda row, total: (1/row["position"]).sum() / total

    df.group_by(["song", "artist"]).apply(calc_popularity, total=len(df))

async def main():
    start = time.time()
    queue1 = asyncio.Queue() 
    queue2= asyncio.Queue()
    dates = date_generator()
    
    async with asyncio.TaskGroup() as tg:
        logging.info("initalized TaskGroup", extra={"second": time.time() - start, "variable": None})
        scrape_task = tg.create_task(scrape_data(t=start, generator=dates, output_queue=queue1))
        clean_task = tg.create_task(clean_data(input_queue=queue1))

    data = clean_task.result()
    df = pd.DataFrame.from_records(data)
    df.to_parquet("hot100.parquet", engine="fastparquet")
    # transform_data(df)



if __name__ == "__main__":
    asyncio.run(main())

