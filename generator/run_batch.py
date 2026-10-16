"""Generate the next batch of parking events, ready to run as a job task.

Differences from calling generate_events.py by hand:
  - the batch number is the highest batch already in the landing folder plus one,
  - the date follows from the batch number (batch 1 is --base-date),
  - the seed is the batch number, so any batch can be reproduced,
  - the memory files (late events, pending duplicates) live next to the data,
    because the compute of a job does not keep local files between runs.

Works on a local folder or on a Unity Catalog volume path, since a volume is
a normal path on Databricks compute.

A Databricks Python script task runs the file with exec(), so __file__ does not
exist there. The folder with generate_events.py is passed with --code-dir.

Example (local test):
    uv run python generator/run_batch.py --root data/local/landing --sessions 100
Example (on Databricks):
    --root /Volumes/parking/landing/events --code-dir /Workspace/Users/<me>/parking-lakehouse/generator
"""

import argparse
import re
import sys
from datetime import date, timedelta
from pathlib import Path

BATCH_PATTERN = re.compile(r"batch_(\d{4})_")


def code_dir() -> Path:
    """Folder that holds generate_events.py: --code-dir, or this file's folder."""
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--code-dir", type=Path)
    known, _ = pre.parse_known_args()
    if known.code_dir:
        return known.code_dir
    if "__file__" in globals():
        return Path(__file__).resolve().parent
    raise SystemExit("--code-dir is required when the script is run without __file__")


sys.path.insert(0, str(code_dir()))
import generate_events  # noqa: E402


def next_batch_number(events_dir: Path) -> int:
    """Highest batch number found in the file names, plus one. 1 if there are none."""
    numbers = []
    if events_dir.exists():
        for path in events_dir.iterdir():
            match = BATCH_PATTERN.match(path.name)
            if match:
                numbers.append(int(match.group(1)))
    return max(numbers, default=0) + 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the next batch of events.")
    parser.add_argument("--root", type=Path, required=True,
                        help="landing root, with parking_events/ and _generator_state/ inside")
    parser.add_argument("--code-dir", type=Path, default=None,
                        help="folder with generate_events.py (needed on Databricks)")
    parser.add_argument("--sessions", type=int, default=100, help="sessions in this batch")
    parser.add_argument("--base-date", type=date.fromisoformat, default=date(2026, 10, 1),
                        help="date of batch 1 (YYYY-MM-DD)")
    parser.add_argument("--new-field-from", type=int, default=6,
                        help="batch from which entry events also carry entry_gate")
    args = parser.parse_args()

    events_dir = args.root / "parking_events"
    state_dir = args.root / "_generator_state"

    batch = next_batch_number(events_dir)
    day = args.base_date + timedelta(days=batch - 1)
    print(f"Next batch: {batch}, date {day}, seed {batch}", flush=True)

    # generate_events.main() reads its arguments from sys.argv.
    sys.argv = [
        "generate_events.py",
        "--sessions", str(args.sessions),
        "--seed", str(batch),
        "--batch", str(batch),
        "--date", day.isoformat(),
        "--new-field-from", str(args.new_field_from),
        "--out-dir", str(events_dir),
        "--pending-file", str(state_dir / "pending_duplicates.jsonl"),
        "--late-file", str(state_dir / "late_events.jsonl"),
    ]
    generate_events.main()


if __name__ == "__main__":
    main()
