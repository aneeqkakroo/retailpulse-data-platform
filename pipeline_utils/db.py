import os

import psycopg
from dotenv import load_dotenv
from pathlib import Path


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)

ENV_FILE = (
    PROJECT_ROOT
    / ".env"
)

load_dotenv(
    ENV_FILE
)


def get_connection():
    return psycopg.connect(
        host=os.getenv(
            "DB_HOST",
            "127.0.0.1",
        ),
        port=int(
            os.getenv(
                "DB_PORT",
                "5433",
            )
        ),
        dbname=os.getenv(
            "DB_NAME",
            "retailpulse",
        ),
        user=os.getenv(
            "DB_USER",
            "retailpulse_admin",
        ),
        password=os.getenv(
            "DB_PASSWORD",
            "retailpulse_dev_password",
        ),
    )