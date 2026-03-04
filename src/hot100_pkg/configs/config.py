import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.getcwd(), os.pardir))

# Database configs
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_PORT = os.getenv("POSTGRES_PORT")

DB_URI = f"postgres+psycopg:///{DB_USER}:{DB_PASSWORD}@db:{DB_PORT}/{DB_NAME}"
