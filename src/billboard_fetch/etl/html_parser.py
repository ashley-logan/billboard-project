from selectolax.parser import HTMLParser
from billboard_fetch.database import Entry
import re


def get_selectors(num: int) -> tuple[str, str, str]:
    # returns css selectors for:
    #   chart position, title, artist
    return (
        f"""
        div.o-chart-results-list-row-container:nth-child({num}) > 
        ul:nth-child(1) > li:nth-child(1) > span:nth-child(1)
        """,
        f"""
    div.o-chart-results-list-row-container:nth-child({num}) > 
    ul:nth-child(1) > li:nth-child(4) > ul:nth-child(1) > 
    li:nth-child(1) > h3:nth-child(1)
    """,
        f"""
    div.o-chart-results-list-row-container:nth-child({num}) > 
    ul:nth-child(1) > li:nth-child(4) > ul:nth-child(1) > 
    li:nth-child(1) > span:nth-child(2)
    """,
    )


def parse_html(html_body: str) -> list[Entry]:
    tree: HTMLParser = HTMLParser(html_body)
    entries: list[Entry] = []
    chart_num: int = 1
    while True:
        position_tag, title_tag, artist_tag = [
            tree.css_first(s) for s in get_selectors(chart_num)
        ]
        if position_tag and title_tag and artist_tag:
            attrs = {
                "position": int(position_tag.text(strip=True)),
                "artist": re.sub(r"(?<! )[aA]nd", " And", artist_tag.text(strip=True)),
                "song_title": title_tag.text(strip=True)
                .replace("RE-\nENTRY", "")
                .replace("NEW", ""),
            }

            entries.append(Entry(**attrs))

            if attrs["position"] == 100 or len(entries) == 100:
                break

        if chart_num >= 150:
            raise Exception("shell html served")

        chart_num += 1

    return entries
