# def get_db_tables() -> list[str]:
#     with Session(engine) as s:
#         inspector = inspect(s.bind)
#         tables = inspector.get_table_names()
#     return tables


# def read_newest() -> DateTime | None:
#     if len(get_db_tables()) == 0:
#         return None
#     with Session(engine) as s:
#         row: Row | None = s.execute(select(func.max(Charts.date))).first()
#     if row is None:
#         return None
#     return row["date"]
