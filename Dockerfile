FROM python:3.14-slim-trixie

WORKDIR /billboard_fetch

RUN pip install uv

RUN uv venv /billboard_fetch/.venv

ENV PATH="/billboard_fetch/.venv/bin:$PATH"

COPY pyproject.toml uv.lock ./

RUN uv sync

COPY . .

ENTRYPOINT [ "python", "./billboard/run.py" ]

CMD [ "fetch-range", "--start=2022-08-24", "--end=2023-08-24" ]

