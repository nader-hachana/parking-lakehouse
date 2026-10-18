"""Generate the next garage/zone snapshot, ready to run as a job task.

The zone feed is a series of full snapshots, one per day. Most days nothing
changed and the snapshot repeats the previous one, which the dimension handles
without creating new versions. Rate changes are scheduled by date, so a
change shows up in the first snapshot on or after its date.

The next snapshot date is the latest snapshot date in the folder plus one day
(or --base-date when the folder is empty).

A Databricks Python script task runs the file with exec(), so __file__ does not
exist there. The folder with generate_garages.py is passed with --code-dir.

Example (local test):
    uv run python generator/run_garages.py --root data/local/landing \\
        --set-rate-from 2026-10-05:G01:Z1=2.75
Example (on Databricks):
    --root /Volumes/parking/landing/events --code-dir /Workspace/Users/<me>/parking-lakehouse/generator \\
        --set-rate-from 2026-10-05:G01:Z1=2.75
"""

import argparse
import json
import re
import sys
from datetime import date, timedelta
from pathlib import Path

SNAPSHOT_PATTERN = re.compile(r"garages_(\d{8})\.jsonl$")


def code_dir() -> Path:
    """Folder that holds generate_garages.py: --code-dir, or this file's folder."""
    pre = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    pre.add_argument("--code-dir", type=Path)
    known, _ = pre.parse_known_args()
    if known.code_dir:
        return known.code_dir
    if "__file__" in globals():
        return Path(__file__).resolve().parent
    raise SystemExit("--code-dir is required when the script is run without __file__")


sys.path.insert(0, str(code_dir()))
from generate_garages import build_snapshot


def latest_snapshot_date(garages_dir: Path) -> date | None:
    dates = []
    if garages_dir.exists():
        for path in garages_dir.iterdir():
            match = SNAPSHOT_PATTERN.match(path.name)
            if match:
                digits = match.group(1)
                dates.append(date.fromisoformat(f"{digits[:4]}-{digits[4:6]}-{digits[6:]}"))
    return max(dates, default=None)


def parse_scheduled_change(value: str) -> tuple[date, str, str, float]:
    """'2026-10-05:G01:Z1=2.75' -> (date(2026, 10, 5), 'G01', 'Z1', 2.75)"""
    when, rest = value.split(":", 1)
    zone, rate = rest.split("=")
    garage_id, zone_id = zone.split(":")
    return date.fromisoformat(when), garage_id, zone_id, float(rate)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the next garage snapshot.")
    parser.add_argument("--root", type=Path, required=True,
                        help="landing root, with garages/ inside")
    parser.add_argument("--code-dir", type=Path, default=None,
                        help="folder with generate_garages.py (needed on Databricks)")
    parser.add_argument("--base-date", type=date.fromisoformat, default=date(2026, 10, 1),
                        help="date of the first snapshot when the folder is empty")
    parser.add_argument("--set-rate-from", action="append", default=[],
                        metavar="DATE:GARAGE:ZONE=RATE",
                        help="from this date on, the zone has this hourly rate (repeatable)")
    args = parser.parse_args()

    garages_dir = args.root / "garages"
    latest = latest_snapshot_date(garages_dir)
    day = args.base_date if latest is None else latest + timedelta(days=1)

    scheduled = sorted(parse_scheduled_change(v) for v in args.set_rate_from)
    # Every change that has already started applies, a later one overrides an earlier one.
    active = [(g, z, r) for when, g, z, r in scheduled if when <= day]

    rows = build_snapshot(day, active)
    garages_dir.mkdir(parents=True, exist_ok=True)
    path = garages_dir / f"garages_{day:%Y%m%d}.jsonl"
    with path.open("w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    print(f"Wrote {len(rows)} zones to {path} (snapshot date {day}, "
          f"{len(active)} rate change(s) in effect)")


if __name__ == "__main__":
    main()
import os
