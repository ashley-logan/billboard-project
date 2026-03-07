FROM astral/uv:python3.14-trixie-slim

WORKDIR /billboard_fetch

RUN pip install uv

COPY pyproject.toml uv.lock ./

RUN uv pip install --system --no-cache

COPY billboard/ /billboard_fetch/

ENTRYPOINT [ "python", "./billboard/run.py" ]

CMD [ "fetch-range", "--start=2022-08-24", "--end=2023-08-24" ]

