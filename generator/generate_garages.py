"""Generate a garage/zone snapshot file (one JSON line per zone).

Each file is a full snapshot: every zone, with its attributes at that moment.
Run it once for the initial state, then again on a later date with --set-rate
to simulate a zone changing its tariff. That change is what the SCD Type 2
dimension has to turn into a new historical version.

Example:
    uv run python generator/generate_garages.py --date 2026-10-01
    uv run python generator/generate_garages.py --date 2026-10-05 --set-rate G01:Z1=2.75
"""

import argparse
import json
from datetime import UTC, date, datetime
from pathlib import Path

from generate_events import GARAGES, TS_FORMAT


def parse_rate_change(value: str) -> tuple[str, str, float]:
    """'G01:Z1=2.75' -> ('G01', 'Z1', 2.75)"""
    zone, rate = value.split("=")
    garage_id, zone_id = zone.split(":")
    return garage_id, zone_id, float(rate)


def build_snapshot(day: date, rate_changes: list[tuple[str, str, float]]) -> list[dict]:
    snapshot_ts = datetime(day.year, day.month, day.day, tzinfo=UTC).strftime(TS_FORMAT)
    changes = {(g, z): r for g, z, r in rate_changes}
    unknown = set(changes) - {(g, z) for g, zones in GARAGES.items() for z in zones}
    if unknown:
        raise SystemExit(f"Unknown zone(s) in --set-rate: {sorted(unknown)}")

    rows = []
    for garage_id, zones in GARAGES.items():
        for position, (zone_id, base_rate) in enumerate(zones.items(), start=1):
            rows.append(
                {
                    "garage_id": garage_id,
                    "zone_id": zone_id,
                    "zone_name": f"{garage_id} zone {zone_id}",
                    "capacity": 30 + 10 * position,
                    "hourly_rate_eur": changes.get((garage_id, zone_id), base_rate),
                    "snapshot_ts": snapshot_ts,
                }
            )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a garage/zone snapshot.")
    parser.add_argument("--date", type=date.fromisoformat, default=date(2026, 10, 1),
                        help="snapshot date (YYYY-MM-DD)")
    parser.add_argument("--set-rate", action="append", default=[], metavar="GARAGE:ZONE=RATE",
                        help="override a zone's hourly rate, can be repeated")
    parser.add_argument("--out-dir", type=Path, default=Path("data/local/garages"),
                        help="local output folder")
    args = parser.parse_args()

    rows = build_snapshot(args.date, [parse_rate_change(v) for v in args.set_rate])

    args.out_dir.mkdir(parents=True, exist_ok=True)
    path = args.out_dir / f"garages_{args.date:%Y%m%d}.jsonl"
    with path.open("w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    print(f"Wrote {len(rows)} zones to {path}")


if __name__ == "__main__":
    main()
