import os
from pathlib import Path

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv


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


if not STORAGE_ACCOUNT:
    raise RuntimeError(
        "AZURE_STORAGE_ACCOUNT is not set."
    )


ACCOUNT_URL = (
    f"https://"
    f"{STORAGE_ACCOUNT}"
    f".dfs.core.windows.net"
)


def get_service_client():
    credential = DefaultAzureCredential()

    return DataLakeServiceClient(
        account_url=ACCOUNT_URL,
        credential=credential,
    )


def upload_file(
    local_path,
    remote_path,
):
    service_client = get_service_client()

    file_system_client = (
        service_client
        .get_file_system_client(
            STORAGE_CONTAINER
        )
    )

    file_client = (
        file_system_client
        .get_file_client(
            remote_path
        )
    )

    with open(
        local_path,
        "rb",
    ) as file_handle:

        file_client.upload_data(
            file_handle,
            overwrite=True,
        )

    print(
        f"Uploaded to ADLS: "
        f"{remote_path}"
    )