import os
from dotenv import load_dotenv, find_dotenv


if path := find_dotenv():
    load_dotenv(path)
else:
    load_dotenv(find_dotenv("template.env"))

# Database configs
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_PORT = os.getenv("POSTGRES_PORT")

# Database connection string
DB_URI = f"postgres+psycopg:///{DB_USER}:{DB_PASSWORD}@db:{DB_PORT}/{DB_NAME}"
