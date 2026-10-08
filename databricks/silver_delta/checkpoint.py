import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

STATE_DIR = PROJECT_ROOT / "state"

CHECKPOINT_FILE = (
    STATE_DIR
    / "silver_checkpoints.json"
)


def load_checkpoints():
    STATE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    if not CHECKPOINT_FILE.exists():
        return {}

    with open(
        CHECKPOINT_FILE,
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


def get_checkpoint(table_name):
    checkpoints = load_checkpoints()

    return checkpoints.get(
        table_name
    )


def save_checkpoint(
    table_name,
    timestamp,
):
    checkpoints = load_checkpoints()

    checkpoints[table_name] = (
        timestamp.isoformat()
    )

    with open(
        CHECKPOINT_FILE,
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            checkpoints,
            file,
            indent=4,
        )