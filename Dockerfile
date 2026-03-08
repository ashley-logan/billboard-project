FROM astral/uv:python3.14-trixie-slim

WORKDIR /billboard-fetch

COPY pyproject.toml uv.lock ./

RUN uv sync --locked --no-install-project

COPY . .

RUN uv sync --locked

ENV PATH=".venv/bin:$PATH"

ENTRYPOINT [ "uv", "run", "fetch" ]

CMD [ "fetch-range", "--start=2022-08-24", "--end=2023-08-24" ]

