import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path


# ==========================================================
# PROJECT SETUP
# ==========================================================

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

POSTGRES_START_COMMAND = [
    "docker",
    "compose",
    "up",
    "-d",
    "postgres",
]

from audit import (
    start_pipeline_run,
    complete_pipeline_run,
    fail_pipeline_run,
)

from db import get_connection

def ensure_postgres_running():
    import time

    print()
    print("Ensuring PostgreSQL is running...")

    subprocess.run(
        [
            "docker",
            "compose",
            "up",
            "-d",
            "postgres",
        ],
        cwd=PROJECT_ROOT,
        check=True,
    )

    print(
        "Waiting for PostgreSQL "
        "to become healthy..."
    )

    max_attempts = 30

    for attempt in range(
        1,
        max_attempts + 1,
    ):

        result = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                "{{.State.Health.Status}}",
                "retailpulse-postgres",
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )

        health = (
            result.stdout.strip()
        )

        if health == "healthy":

            print(
                "PostgreSQL is healthy."
            )

            return

        print(
            f"Waiting for PostgreSQL... "
            f"({attempt}/{max_attempts})"
        )

        time.sleep(
            2
        )

    raise RuntimeError(
        "PostgreSQL did not become "
        "healthy within 60 seconds."
    )


# ==========================================================
# PIPELINE STEP DEFINITIONS
# ==========================================================

BRONZE_COMMAND = [
    sys.executable,
    str(
        PROJECT_ROOT
        / "ingestion"
        / "bronze"
        / "extract.py"
    ),
]


SILVER_COMMAND = [
    "docker",
    "compose",
    "run",
    "--rm",
    "spark",
    "python",
    "databricks/silver_delta/merge.py",
]


GOLD_COMMAND = [
    "docker",
    "compose",
    "run",
    "--rm",
    "spark",
    "python",
    "databricks/gold/build_gold.py",
]


SIMULATE_COMMAND = [
    sys.executable,
    str(
        PROJECT_ROOT
        / "data-generator"
        / "daily_updates.py"
    ),
]


# ==========================================================
# LOGGING HELPERS
# ==========================================================

def print_header(title):
    print()
    print("=" * 75)
    print(title)
    print("=" * 75)


def print_step(
    step_number,
    total_steps,
    name,
):
    print()
    print("-" * 75)

    print(
        f"[{step_number}/{total_steps}] "
        f"{name}"
    )

    print("-" * 75)


# ==========================================================
# COMMAND RUNNER
# ==========================================================

def run_command(
    name,
    command,
):
    """
    Run one pipeline command.

    stdout/stderr are streamed directly to the terminal.

    Any non-zero exit code causes the orchestration
    pipeline to fail immediately.
    """

    print()
    print(
        f"Starting: {name}"
    )

    print(
        "Command: "
        + " ".join(
            str(part)
            for part in command
        )
    )

    started_at = datetime.now()

    try:

        subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            check=True,
        )

    except subprocess.CalledProcessError as error:

        elapsed = (
            datetime.now()
            - started_at
        ).total_seconds()

        print()
        print(
            f"{name}: FAILED"
        )

        print(
            f"Duration: "
            f"{elapsed:.2f} seconds"
        )

        raise RuntimeError(
            f"{name} failed with "
            f"exit code "
            f"{error.returncode}"
        ) from error

    elapsed = (
        datetime.now()
        - started_at
    ).total_seconds()

    print()
    print(
        f"{name}: SUCCESS"
    )

    print(
        f"Duration: "
        f"{elapsed:.2f} seconds"
    )


# ==========================================================
# PARENT AUDIT
# ==========================================================

def start_parent_audit():
    with get_connection() as connection:

        with connection.cursor() as cursor:

            run_id = start_pipeline_run(
                cursor=cursor,
                pipeline_name=(
                    "retailpulse_end_to_end"
                ),
                layer="ORCHESTRATION",
                source_name=(
                    "PostgreSQL retail source"
                ),
                target_name=(
                    "RetailPulse Gold Lakehouse"
                ),
            )

            connection.commit()

    return run_id


def complete_parent_audit(
    run_id,
):
    with get_connection() as connection:

        with connection.cursor() as cursor:

            complete_pipeline_run(
                cursor=cursor,
                run_id=run_id,
                rows_read=0,
                rows_written=0,
                rows_quarantined=0,
                watermark_from=None,
                watermark_to=None,
            )

            connection.commit()


def fail_parent_audit(
    run_id,
    error,
):
    with get_connection() as connection:

        with connection.cursor() as cursor:

            fail_pipeline_run(
                cursor=cursor,
                run_id=run_id,
                error=error,
                rows_read=0,
                rows_written=0,
                rows_quarantined=0,
                watermark_from=None,
                watermark_to=None,
            )

            connection.commit()


# ==========================================================
# ARGUMENTS
# ==========================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description=(
            "Run the RetailPulse "
            "end-to-end data pipeline."
        )
    )

    parser.add_argument(
        "--simulate",
        action="store_true",
        help=(
            "Generate simulated daily source "
            "changes before running Bronze."
        ),
    )

    parser.add_argument(
        "--skip-gold",
        action="store_true",
        help=(
            "Run Bronze and Silver only."
        ),
    )

    return parser.parse_args()


# ==========================================================
# MAIN ORCHESTRATION
# ==========================================================

def main():

    args = parse_arguments()

    pipeline_started_at = (
        datetime.now()
    )

    parent_run_id = None

    print_header(
        "RetailPulse End-to-End Data Pipeline"
    )

    print(
        f"Project: {PROJECT_ROOT}"
    )

    print(
        f"Started: "
        f"{pipeline_started_at}"
    )

    print(
        f"Simulate source changes: "
        f"{args.simulate}"
    )

    try:

        # --------------------------------------------------
        # Ensure infrastructure is running
        # --------------------------------------------------

        ensure_postgres_running()

        # --------------------------------------------------
        # Parent audit record
        # --------------------------------------------------

        parent_run_id = (
            start_parent_audit()
        )

        print(
            f"Parent audit run ID: "
            f"{parent_run_id}"
        )

        # --------------------------------------------------
        # Build execution plan
        # --------------------------------------------------

        steps = []

        if args.simulate:

            steps.append(
                (
                    "Simulate source changes",
                    SIMULATE_COMMAND,
                )
            )

        steps.append(
            (
                "Bronze ingestion",
                BRONZE_COMMAND,
            )
        )

        steps.append(
            (
                "Silver Delta MERGE",
                SILVER_COMMAND,
            )
        )

        if not args.skip_gold:

            steps.append(
                (
                    "Gold dimensional model",
                    GOLD_COMMAND,
                )
            )

        # --------------------------------------------------
        # Execute sequentially
        # --------------------------------------------------

        total_steps = len(
            steps
        )

        for index, (
            step_name,
            command,
        ) in enumerate(
            steps,
            start=1,
        ):

            print_step(
                index,
                total_steps,
                step_name,
            )

            run_command(
                step_name,
                command,
            )

        # --------------------------------------------------
        # Success
        # --------------------------------------------------

        complete_parent_audit(
            parent_run_id
        )

        duration = (
            datetime.now()
            - pipeline_started_at
        ).total_seconds()

        print_header(
            "PIPELINE SUCCESS"
        )

        print(
            f"Run ID: "
            f"{parent_run_id}"
        )

        print(
            f"Total duration: "
            f"{duration:.2f} seconds"
        )

        print(
            "Bronze: SUCCESS"
        )

        print(
            "Silver: SUCCESS"
        )

        if not args.skip_gold:

            print(
                "Gold: SUCCESS"
            )

    except Exception as error:

        # --------------------------------------------------
        # Failure
        # --------------------------------------------------

        if parent_run_id is not None:

            try:

                fail_parent_audit(
                    parent_run_id,
                    error,
                )

            except Exception as audit_error:

                print()
                print(
                    "WARNING: Parent failure "
                    "audit could not be saved."
                )

                print(
                    f"Audit error: "
                    f"{audit_error}"
                )

        duration = (
            datetime.now()
            - pipeline_started_at
        ).total_seconds()

        print_header(
            "PIPELINE FAILED"
        )

        print(
            f"Error: {error}"
        )

        print(
            f"Duration: "
            f"{duration:.2f} seconds"
        )

        # Important:
        # return non-zero exit code for CI / schedulers.
        sys.exit(1)


if __name__ == "__main__":
    main()