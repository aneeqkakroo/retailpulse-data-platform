from pathlib import Path
import sys


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)

sys.path.append(
    str(
        PROJECT_ROOT
        / "pipeline_utils"
    )
)

from adls import upload_file


BRONZE_ROOT = (
    PROJECT_ROOT
    / "bronze"
)


def main():
    parquet_files = list(
        BRONZE_ROOT.rglob(
            "*.parquet"
        )
    )

    print(
        f"Bronze files found: "
        f"{len(parquet_files):,}"
    )

    if not parquet_files:
        print(
            "No local Bronze Parquet files found."
        )
        return

    for index, local_path in enumerate(
        parquet_files,
        start=1,
    ):
        remote_path = (
            local_path
            .relative_to(
                PROJECT_ROOT
            )
            .as_posix()
        )

        print(
            f"[{index}/{len(parquet_files)}] "
            f"{remote_path}"
        )

        upload_file(
            local_path=local_path,
            remote_path=remote_path,
        )

    print()
    print(
        "Bronze ADLS backfill completed."
    )


if __name__ == "__main__":
    main()