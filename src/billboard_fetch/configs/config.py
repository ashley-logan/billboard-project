import os
from dotenv import load_dotenv

ENV_DIR = os.path.join("/billboard-fetch", "docker")

# load user created .env if set else load pre-populated template.env
if not load_dotenv(dotenv_path=os.path.join(ENV_DIR, ".env")):
    load_dotenv(dotenv_path=os.path.join(ENV_DIR, "template.env"))

# Database configs
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")

# Database connection string
DB_URI = f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@db:5432/{DB_NAME}"
