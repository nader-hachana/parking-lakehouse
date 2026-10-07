"""Generate clean parking events as one JSON Lines file.

Each parking session produces three linked events: entry, payment, exit.
Problems (duplicates, late events, bad values, schema change) are added in
later steps, so this version only writes clean data.

Example:
    uv run python generator/generate_events.py --sessions 50 --seed 1
"""

import argparse
import json
import math
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# garage_id -> {zone_id: hourly rate in EUR}
GARAGES = {
    "G01": {"Z1": 2.00, "Z2": 3.00},
    "G02": {"Z1": 2.50, "Z2": 3.50, "Z3": 4.00},
    "G03": {"Z1": 1.80},
}
PAYMENT_METHODS = ["card", "cash", "app"]
PLATE_LETTERS = "ABCDEFGHJKLMNPRSTUVWXYZ"


def make_id(rng: random.Random) -> str:
    """UUID from the seeded generator, so the same seed gives the same ids."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def format_ts(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def make_plate(rng: random.Random) -> str:
    return f"W-{rng.randint(10000, 99999)}{rng.choice(PLATE_LETTERS)}"


def make_session(rng: random.Random, day_start: datetime) -> list[dict]:
    """One visit: entry, then payment shortly before leaving, then exit."""
    garage_id = rng.choice(list(GARAGES))
    zone_id = rng.choice(list(GARAGES[garage_id]))
    plate = make_plate(rng)
    session_id = make_id(rng)

    entry_ts = day_start + timedelta(seconds=rng.randint(0, 24 * 3600 - 1))
    duration_min = rng.randint(10, 480)
    exit_ts = entry_ts + timedelta(minutes=duration_min)
    payment_ts = exit_ts - timedelta(minutes=rng.randint(1, 5))

    # Billed per started hour.
    amount = math.ceil(duration_min / 60) * GARAGES[garage_id][zone_id]

    base = {
        "session_id": session_id,
        "garage_id": garage_id,
        "zone_id": zone_id,
        "plate": plate,
    }
    return [
        {"event_id": make_id(rng), "event_type": "entry", "event_ts": format_ts(entry_ts), **base},
        {
            "event_id": make_id(rng),
            "event_type": "payment",
            "event_ts": format_ts(payment_ts),
            **base,
            "amount_eur": round(amount, 2),
            "payment_method": rng.choice(PAYMENT_METHODS),
        },
        {"event_id": make_id(rng), "event_type": "exit", "event_ts": format_ts(exit_ts), **base},
    ]


def generate(sessions: int, seed: int, day: date) -> list[dict]:
    rng = random.Random(seed)
    day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    events = []
    for _ in range(sessions):
        events.extend(make_session(rng, day_start))
    # Files arrive roughly in time order.
    events.sort(key=lambda e: e["event_ts"])
    return events


def write_batch(events: list[dict], out_dir: Path, batch: int) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    path = out_dir / f"batch_{batch:04d}_{stamp}.jsonl"
    with path.open("w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate parking events.")
    parser.add_argument("--sessions", type=int, default=50, help="number of parking sessions")
    parser.add_argument("--seed", type=int, default=1, help="random seed, same seed gives same content")
    parser.add_argument("--batch", type=int, default=1, help="batch number used in the file name")
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=date(2026, 10, 1),
        help="day the events happen on (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/local/parking_events"),
        help="local output folder",
    )
    args = parser.parse_args()

    events = generate(args.sessions, args.seed, args.date)
    path = write_batch(events, args.out_dir, args.batch)
    print(f"Wrote {len(events)} events to {path}")


if __name__ == "__main__":
    main()
