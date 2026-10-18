"""Tests for the building blocks of generate_events.py. Pure Python, no Spark or Databricks."""

import json
import math
import sys
from collections import Counter, defaultdict
from datetime import date, datetime

import generate_events as ge
import pytest

DAY = date(2026, 10, 1)


@pytest.fixture
def events():
    return ge.generate(sessions=200, seed=1, day=DAY)


def ts(value: str) -> datetime:
    return datetime.strptime(value, ge.TS_FORMAT)


def by_session(events):
    sessions = defaultdict(dict)
    for event in events:
        sessions[event["session_id"]][event["event_type"]] = event
    return sessions


# --- clean data -------------------------------------------------------------


def test_generate_is_deterministic_for_a_seed():
    assert ge.generate(50, 1, DAY) == ge.generate(50, 1, DAY)
    assert ge.generate(50, 1, DAY) != ge.generate(50, 2, DAY)


def test_every_session_has_entry_payment_exit_in_that_order(events):
    sessions = by_session(events)
    assert len(sessions) == 200
    for parts in sessions.values():
        assert set(parts) == {"entry", "payment", "exit"}
        assert parts["entry"]["event_ts"] < parts["payment"]["event_ts"] < parts["exit"]["event_ts"]


def test_event_ids_are_unique(events):
    ids = [e["event_id"] for e in events]
    assert len(ids) == len(set(ids))


def test_events_come_in_time_order(events):
    stamps = [e["event_ts"] for e in events]
    assert stamps == sorted(stamps)


def test_amount_is_billed_per_started_hour(events):
    for parts in by_session(events).values():
        entry, payment, exit_ = parts["entry"], parts["payment"], parts["exit"]
        minutes = (ts(exit_["event_ts"]) - ts(entry["event_ts"])).total_seconds() / 60
        rate = ge.GARAGES[entry["garage_id"]][entry["zone_id"]]
        assert payment["amount_eur"] == round(math.ceil(minutes / 60) * rate, 2)


# --- late events ------------------------------------------------------------


def test_late_events_keep_their_time_and_come_back_one_to_three_batches_later(events):
    kept, held = ge.hold_back_late(events, rate=0.05, seed=1, batch=3)
    assert len(held) == round(len(events) * 0.05)
    assert len(kept) + len(held) == len(events)
    assert all(4 <= record["release_batch"] <= 6 for record in held)

    originals = {e["event_id"]: e for e in events}
    held_ids = {record["event"]["event_id"] for record in held}
    assert held_ids.isdisjoint(e["event_id"] for e in kept)
    assert all(record["event"] == originals[record["event"]["event_id"]] for record in held)


def test_no_late_events_when_the_rate_is_zero(events):
    kept, held = ge.hold_back_late(events, rate=0, seed=1, batch=1)
    assert kept == events and held == []


def test_split_late_releases_only_what_is_due():
    records = [
        {"release_batch": 3, "event": {"event_id": "a"}},
        {"release_batch": 5, "event": {"event_id": "b"}},
    ]
    due, waiting = ge.split_late(records, batch=3)
    assert [e["event_id"] for e in due] == ["a"]
    assert waiting == [records[1]]


# --- duplicates -------------------------------------------------------------


def test_duplicates_are_exact_copies_inside_the_file_and_carried_over(events):
    out, carry = ge.inject_duplicates(events, rate=0.1, seed=1)
    originals = {e["event_id"]: e for e in events}
    wanted = round(len(events) * 0.1)

    assert all(e == originals[e["event_id"]] for e in out)
    assert all(e == originals[e["event_id"]] for e in carry)
    assert len(carry) == wanted // 2
    assert len(out) == len(events) + (wanted - len(carry))
    assert max(Counter(e["event_id"] for e in out).values()) == 2


def test_a_duplicate_never_comes_before_its_original(events):
    out, _ = ge.inject_duplicates(events, rate=0.1, seed=1)
    first_index = {}
    for index, event in enumerate(out):
        first_index.setdefault(event["event_id"], index)
    originals_order = [e["event_id"] for e in events]
    assert sorted(first_index, key=first_index.get) == originals_order


def test_no_duplicates_when_the_rate_is_zero(events):
    out, carry = ge.inject_duplicates(events, rate=0, seed=1)
    assert out == events and carry == []


# --- missing, invalid and malformed ----------------------------------------


def test_an_event_never_gets_two_problems(events):
    out, n_missing, n_invalid = ge.corrupt_events(events, missing_rate=0.05, invalid_rate=0.03, seed=1)
    originals = {e["event_id"]: e for e in events}
    entry_ts = {s: p["entry"]["event_ts"] for s, p in by_session(events).items()}

    missing = invalid = 0
    for e in out:
        no_plate = "plate" not in e
        null_amount = e["event_type"] == "payment" and e["amount_eur"] is None
        negative = e["event_type"] == "payment" and e["amount_eur"] is not None and e["amount_eur"] < 0
        exit_before_entry = e["event_type"] == "exit" and e["event_ts"] < entry_ts[e["session_id"]]

        assert sum([no_plate, null_amount, negative, exit_before_entry]) <= 1
        missing += no_plate or null_amount
        invalid += negative or exit_before_entry
        if not (no_plate or null_amount or negative or exit_before_entry):
            assert e == originals[e["event_id"]]

    assert (missing, invalid) == (n_missing, n_invalid)
    assert n_missing == round(len(events) * 0.05)
    assert n_invalid == round(len(events) * 0.03)


def test_nothing_is_corrupted_when_the_rates_are_zero(events):
    out, n_missing, n_invalid = ge.corrupt_events(events, missing_rate=0, invalid_rate=0, seed=1)
    assert out == events and (n_missing, n_invalid) == (0, 0)


def test_malformed_lines_are_cut_off_json(events):
    lines, count = ge.to_lines(events, malformed_rate=0.02, seed=1)
    broken = 0
    for line in lines:
        try:
            json.loads(line)
        except json.JSONDecodeError:
            broken += 1
    assert len(lines) == len(events)
    assert broken == count == round(len(events) * 0.02)


# --- schema changes ---------------------------------------------------------


@pytest.mark.parametrize(
    ("function", "field", "allowed"),
    [
        (ge.add_vehicle_type, "vehicle_type", ge.VEHICLE_TYPES),
        (ge.add_entry_gate, "entry_gate", ge.ENTRY_GATES),
    ],
)
def test_a_new_field_only_goes_on_entry_events_and_leaves_the_input_alone(events, function, field, allowed):
    out = function(events, seed=1)
    for e in out:
        if e["event_type"] == "entry":
            assert e[field] in allowed
        else:
            assert field not in e
    assert all(field not in e for e in events)


# --- state files ------------------------------------------------------------


def test_state_files_are_rewritten_not_appended(tmp_path):
    path = tmp_path / "state" / "pending.jsonl"
    ge.write_pending(path, [{"event_id": "a"}])
    ge.write_pending(path, [{"event_id": "b"}])
    assert ge.read_pending(path) == [{"event_id": "b"}]


def test_writing_no_state_removes_the_file(tmp_path):
    path = tmp_path / "late.jsonl"
    ge.write_late(path, [{"release_batch": 2, "event": {"event_id": "a"}}])
    assert path.exists()
    ge.write_late(path, [])
    assert not path.exists()


def test_reading_missing_state_gives_nothing(tmp_path):
    assert ge.read_late(tmp_path / "nope.jsonl") == []
    assert ge.read_pending(tmp_path / "nope.jsonl") == []


# --- the script as a whole --------------------------------------------------

NO_PROBLEMS = [
    "--duplicate-rate", 0, "--late-rate", 0, "--missing-rate", 0, "--invalid-rate", 0, "--malformed-rate", 0,
]  # fmt: skip


def run_main(monkeypatch, *args):
    monkeypatch.setattr(sys, "argv", ["generate_events.py", *map(str, args)])
    ge.main()


def only_file(folder):
    (path,) = list(folder.glob("*.jsonl"))
    return path


def test_main_gives_the_same_content_for_the_same_arguments(tmp_path, monkeypatch):
    for name in ("a", "b"):
        run_main(
            monkeypatch, "--sessions", 40, "--seed", 3, "--batch", 3, "--date", "2026-10-03",
            "--out-dir", tmp_path / name / "out",
            "--pending-file", tmp_path / name / "p.jsonl",
            "--late-file", tmp_path / name / "l.jsonl",
        )  # fmt: skip
    assert only_file(tmp_path / "a" / "out").read_text() == only_file(tmp_path / "b" / "out").read_text()


@pytest.mark.parametrize(
    ("batch", "has_vehicle_type", "has_entry_gate"),
    [(4, False, False), (5, True, False), (6, True, True)],
)
def test_main_switches_the_schema_at_the_configured_batches(
    tmp_path, monkeypatch, batch, has_vehicle_type, has_entry_gate
):
    run_main(
        monkeypatch, "--sessions", 20, "--seed", batch, "--batch", batch, "--date", "2026-10-05",
        "--new-field-from", 6, "--out-dir", tmp_path / "out",
        "--pending-file", tmp_path / "p.jsonl", "--late-file", tmp_path / "l.jsonl", *NO_PROBLEMS,
    )  # fmt: skip
    entries = [
        e for e in map(json.loads, only_file(tmp_path / "out").read_text().splitlines())
        if e["event_type"] == "entry"
    ]  # fmt: skip
    assert entries
    assert all(("vehicle_type" in e) is has_vehicle_type for e in entries)
    assert all(("entry_gate" in e) is has_entry_gate for e in entries)


def test_a_failed_write_leaves_the_state_untouched(tmp_path, monkeypatch):
    """The state is committed only after the batch file exists, so a failed run can be retried."""
    state = tmp_path / "state"
    common = [
        "--sessions", 40, "--date", "2026-10-02",
        "--pending-file", state / "p.jsonl", "--late-file", state / "l.jsonl",
    ]  # fmt: skip
    run_main(monkeypatch, *common, "--seed", 1, "--batch", 1, "--out-dir", tmp_path / "out")
    before = {name: (state / name).read_bytes() for name in ("p.jsonl", "l.jsonl")}
    assert all(before.values())

    blocker = tmp_path / "blocker"
    blocker.write_text("a file where the output folder should be")
    with pytest.raises(OSError):
        run_main(monkeypatch, *common, "--seed", 2, "--batch", 2, "--out-dir", blocker)

    assert before == {name: (state / name).read_bytes() for name in ("p.jsonl", "l.jsonl")}
