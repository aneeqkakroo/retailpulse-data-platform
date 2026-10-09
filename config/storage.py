import os
from dotenv import load_dotenv
from pathlib import Path


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)

load_dotenv(
    PROJECT_ROOT / ".env"
)


STORAGE_ACCOUNT = os.getenv(
    "AZURE_STORAGE_ACCOUNT"
)

STORAGE_CONTAINER = os.getenv(
    "AZURE_STORAGE_CONTAINER",
    "retailpulse",
)


def adls_base_path():
    return (
        f"abfss://"
        f"{STORAGE_CONTAINER}"
        f"@"
        f"{STORAGE_ACCOUNT}"
        f".dfs.core.windows.net"
    )


def bronze_path(table_name):
    return (
        f"{adls_base_path()}"
        f"/bronze/"
        f"{table_name}"
    )


def silver_path(table_name):
    return (
        f"{adls_base_path()}"
        f"/silver/"
        f"{table_name}"
    )


def gold_path(table_name):
    return (
        f"{adls_base_path()}"
        f"/gold/"
        f"{table_name}"
    )