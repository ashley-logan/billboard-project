import duckdb
import pandas as pd
import asyncio
import aiohttp

# --- DuckDB setup ---
con = duckdb.connect(":memory:")
con.execute("CREATE TABLE IF NOT EXISTS data (id INT, value DOUBLE)")

# --- Async upstream (e.g., API calls) ---
async def fetch_data():
    urls = [f"https://api.example.com/page={i}" for i in range(10)]
    async with aiohttp.ClientSession() as session:
        for url in urls:
            async with session.get(url) as resp:
                result = await resp.json()
                for row in result['results']:
                    yield row

# --- Buffering logic (plain list) ---
async def main():
    buffer = []
    BUFFER_SIZE = 500

    async for item in fetch_data():
        buffer.append({"id": item["id"], "value": item["value"]})

        if len(buffer) >= BUFFER_SIZE:
            df = pd.DataFrame(buffer)
            con.execute("INSERT INTO data SELECT * FROM df", {"df": df})
            buffer.clear()

    # Final flush
    if buffer:
        df = pd.DataFrame(buffer)
        con.execute("INSERT INTO data SELECT * FROM df", {"df": df})

    print(con.execute("SELECT COUNT(*) FROM data").fetchone())

asyncio.run(main())