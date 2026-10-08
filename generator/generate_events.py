"""Generate parking events as one JSON Lines file per batch.

Each parking session produces three linked events: entry, payment, exit.
On top of the clean data, problems are injected on purpose:
  - duplicates (same event_id), inside a file and across batches
  - late events: event_ts is honest, but delivery is 1 to 3 batches late
  - missing fields (no plate, null amount), invalid values (negative amount,
    exit before entry) and malformed lines (cut-off, broken JSON)
  - a schema change: from a chosen batch on, entry events get a new field
    `vehicle_type`. Older batches, and late events held back before the
    change, do not have it.

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
VEHICLE_TYPES = ["car", "ev", "motorcycle"]
PLATE_LETTERS = "ABCDEFGHJKLMNPRSTUVWXYZ"


def make_id(rng: random.Random) -> str:
    """UUID from the seeded generator, so the same seed gives the same ids."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def format_ts(ts: datetime) -> str:
    return ts.strftime(TS_FORMAT)


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


def hold_back_late(
    events: list[dict], rate: float, seed: int, batch: int
) -> tuple[list[dict], list[dict]]:
    """Take a share of events out of this batch and deliver them 1 to 3 batches
    later. event_ts is not changed, so the event arrives after newer data.

    Returns the events that stay in this batch, and records to store as
    {"release_batch": n, "event": {...}}.
    """
    rng = random.Random(f"{seed}-late")
    count = round(len(events) * rate)
    late_idx = set(rng.sample(range(len(events)), k=min(count, len(events))))

    kept = [e for i, e in enumerate(events) if i not in late_idx]
    held = [
        {"release_batch": batch + rng.randint(1, 3), "event": events[i]}
        for i in sorted(late_idx)
    ]
    return kept, held


def release_late(path: Path, batch: int) -> list[dict]:
    """Return held events that are due in this batch. The rest stay in the file."""
    if not path.exists():
        return []
    with path.open() as f:
        records = [json.loads(line) for line in f if line.strip()]
    due = [r["event"] for r in records if r["release_batch"] <= batch]
    waiting = [r for r in records if r["release_batch"] > batch]
    if waiting:
        with path.open("w") as f:
            for record in waiting:
                f.write(json.dumps(record) + "\n")
    else:
        path.unlink()
    return due


def save_late(path: Path, held: list[dict]) -> None:
    if not held:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        for record in held:
            f.write(json.dumps(record) + "\n")


def inject_duplicates(
    events: list[dict], rate: float, seed: int
) -> tuple[list[dict], list[dict]]:
    """Copy a share of events. Half reappear shortly after in the same file,
    half are returned as carry-over for a later batch (an upstream retry).

    A duplicate is an exact copy with the same event_id, which is what silver
    will deduplicate on.
    """
    rng = random.Random(f"{seed}-duplicates")
    count = round(len(events) * rate)
    chosen = rng.sample(range(len(events)), k=min(count, len(events)))

    keyed = [(float(i), event) for i, event in enumerate(events)]
    carry_over = []
    for n, i in enumerate(chosen):
        if n % 2 == 0:
            # Same file: lands a few positions after the original.
            keyed.append((i + rng.uniform(0.5, 30), dict(events[i])))
        else:
            carry_over.append(dict(events[i]))

    keyed.sort(key=lambda pair: pair[0])
    return [event for _, event in keyed], carry_over


def load_pending(path: Path) -> list[dict]:
    """Read carry-over duplicates from an earlier run, then clear the file."""
    if not path.exists():
        return []
    with path.open() as f:
        pending = [json.loads(line) for line in f if line.strip()]
    path.unlink()
    return pending


def save_pending(path: Path, events: list[dict]) -> None:
    if not events:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")


def corrupt_events(
    events: list[dict], missing_rate: float, invalid_rate: float, seed: int
) -> tuple[list[dict], int, int]:
    """Make some events incomplete or wrong, still valid JSON.

    Missing: `plate` removed, or `amount_eur` set to null on a payment.
    Invalid: negative `amount_eur`, or an exit stamped before its entry.
    An event gets at most one of these.
    """
    rng = random.Random(f"{seed}-corrupt")
    events = [dict(e) for e in events]
    entry_ts = {e["session_id"]: e["event_ts"] for e in events if e["event_type"] == "entry"}

    missing_idx = rng.sample(
        range(len(events)), k=min(round(len(events) * missing_rate), len(events))
    )
    for i in missing_idx:
        if events[i]["event_type"] == "payment" and rng.random() < 0.5:
            events[i]["amount_eur"] = None
        else:
            events[i].pop("plate", None)

    # Only payments and exits with a known entry can be made invalid.
    taken = set(missing_idx)
    eligible = [
        i
        for i, e in enumerate(events)
        if i not in taken
        and (e["event_type"] == "payment" or (e["event_type"] == "exit" and e["session_id"] in entry_ts))
    ]
    invalid_idx = rng.sample(
        eligible, k=min(round(len(events) * invalid_rate), len(eligible))
    )
    for i in invalid_idx:
        e = events[i]
        if e["event_type"] == "payment":
            e["amount_eur"] = -abs(e["amount_eur"])
        else:
            entry = datetime.strptime(entry_ts[e["session_id"]], TS_FORMAT)
            e["event_ts"] = format_ts(entry - timedelta(minutes=rng.randint(1, 60)))

    return events, len(missing_idx), len(invalid_idx)


def to_lines(events: list[dict], malformed_rate: float, seed: int) -> tuple[list[str], int]:
    """Turn events into JSON lines. A share of lines is cut off, so they are
    not valid JSON anymore and the event is lost (like a broken upload)."""
    rng = random.Random(f"{seed}-malformed")
    lines = [json.dumps(e) for e in events]
    count = min(round(len(lines) * malformed_rate), len(lines))
    for i in rng.sample(range(len(lines)), k=count):
        cut = int(len(lines[i]) * rng.uniform(0.3, 0.8))
        lines[i] = lines[i][:cut]
    return lines, count


def add_vehicle_type(events: list[dict], seed: int) -> list[dict]:
    """Schema change: entry events get a new field, `vehicle_type`."""
    rng = random.Random(f"{seed}-vehicle")
    result = []
    for event in events:
        event = dict(event)
        if event["event_type"] == "entry":
            event["vehicle_type"] = rng.choice(VEHICLE_TYPES)
        result.append(event)
    return result


def write_batch(lines: list[str], out_dir: Path, batch: int) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    path = out_dir / f"batch_{batch:04d}_{stamp}.jsonl"
    with path.open("w") as f:
        for line in lines:
            f.write(line + "\n")
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
    parser.add_argument(
        "--duplicate-rate",
        type=float,
        default=0.02,
        help="share of events that get duplicated (0 turns it off)",
    )
    parser.add_argument(
        "--pending-file",
        type=Path,
        default=Path("data/local/pending_duplicates.jsonl"),
        help="carry-over duplicates that show up in the next batch",
    )
    parser.add_argument(
        "--late-rate",
        type=float,
        default=0.03,
        help="share of events delivered 1 to 3 batches late (0 turns it off)",
    )
    parser.add_argument(
        "--late-file",
        type=Path,
        default=Path("data/local/late_events.jsonl"),
        help="events waiting for a later batch",
    )
    parser.add_argument(
        "--missing-rate",
        type=float,
        default=0.015,
        help="share of events with a missing plate or a null amount",
    )
    parser.add_argument(
        "--invalid-rate",
        type=float,
        default=0.005,
        help="share of events with a negative amount or an exit before its entry",
    )
    parser.add_argument(
        "--malformed-rate",
        type=float,
        default=0.005,
        help="share of lines written as broken JSON",
    )
    parser.add_argument(
        "--schema-version-from",
        type=int,
        default=5,
        help="from this batch number on, entry events get a `vehicle_type` field",
    )
    args = parser.parse_args()

    events = generate(args.sessions, args.seed, args.date)
    schema_changed = args.batch >= args.schema_version_from
    if schema_changed:
        events = add_vehicle_type(events, args.seed)

    # Late events leave this batch first, so they are not duplicated here.
    events, late_held = hold_back_late(events, args.late_rate, args.seed, args.batch)
    # Bad values are added before duplicating, so a duplicate is an exact copy.
    events, n_missing, n_invalid = corrupt_events(
        events, args.missing_rate, args.invalid_rate, args.seed
    )
    events, carry_over = inject_duplicates(events, args.duplicate_rate, args.seed)

    # Events held back by earlier runs arrive at the start of this file.
    late_due = release_late(args.late_file, args.batch)
    pending = load_pending(args.pending_file)
    events = late_due + pending + events
    save_late(args.late_file, late_held)
    save_pending(args.pending_file, carry_over)

    lines, n_malformed = to_lines(events, args.malformed_rate, args.seed)
    path = write_batch(lines, args.out_dir, args.batch)
    print(
        f"Wrote {len(lines)} lines to {path}\n"
        f"  late events arriving now: {len(late_due)}, held back for later: {len(late_held)}\n"
        f"  duplicates carried over: {len(pending)}, held for the next batch: {len(carry_over)}\n"
        f"  missing fields: {n_missing}, invalid values: {n_invalid}, malformed lines: {n_malformed}\n"
        f"  schema: {'new (entry events have vehicle_type)' if schema_changed else 'original'}"
    )


if __name__ == "__main__":
    main()
