"""Tests for the garage and zone snapshots (generate_garages.py and run_garages.py)."""

import json
import sys
from datetime import date

import generate_events as ge
import generate_garages as gg
import pytest
import run_garages as rg

ZONES = sum(len(zones) for zones in ge.GARAGES.values())


def test_a_snapshot_has_every_zone_with_its_base_rate():
    rows = gg.build_snapshot(date(2026, 10, 5), [])
    assert len(rows) == ZONES
    assert {r["snapshot_ts"] for r in rows} == {"2026-10-05T00:00:00Z"}
    for row in rows:
        assert row["hourly_rate_eur"] == ge.GARAGES[row["garage_id"]][row["zone_id"]]
        assert row["zone_name"] == f"{row['garage_id']} zone {row['zone_id']}"


def test_a_rate_change_only_touches_its_zone():
    base = {(r["garage_id"], r["zone_id"]): r for r in gg.build_snapshot(date(2026, 10, 5), [])}
    changed = {(r["garage_id"], r["zone_id"]): r for r in gg.build_snapshot(date(2026, 10, 5), [("G01", "Z1", 2.75)])}
    assert changed[("G01", "Z1")]["hourly_rate_eur"] == 2.75
    for key, row in base.items():
        if key != ("G01", "Z1"):
            assert changed[key] == row


def test_an_unknown_zone_is_rejected():
    with pytest.raises(SystemExit):
        gg.build_snapshot(date(2026, 10, 5), [("G09", "Z1", 1.0)])


def test_rate_changes_are_parsed():
    assert gg.parse_rate_change("G01:Z1=2.75") == ("G01", "Z1", 2.75)
    assert rg.parse_scheduled_change("2026-10-05:G01:Z1=2.75") == (date(2026, 10, 5), "G01", "Z1", 2.75)


def test_latest_snapshot_date_ignores_other_files(tmp_path):
    assert rg.latest_snapshot_date(tmp_path / "missing") is None
    (tmp_path / "garages_20261001.jsonl").touch()
    (tmp_path / "garages_20261003.jsonl").touch()
    (tmp_path / "notes.txt").touch()
    assert rg.latest_snapshot_date(tmp_path) == date(2026, 10, 3)


def run_snapshot(monkeypatch, root, *extra):
    monkeypatch.setattr(sys, "argv", ["run_garages.py", "--root", str(root), *extra])
    rg.main()


def rate_of_g01_z1(path):
    for line in path.read_text().splitlines():
        row = json.loads(line)
        if (row["garage_id"], row["zone_id"]) == ("G01", "Z1"):
            return row["hourly_rate_eur"]


def test_the_runner_writes_one_snapshot_a_day_and_applies_a_scheduled_change_from_its_date(tmp_path, monkeypatch):
    for _ in range(6):
        run_snapshot(monkeypatch, tmp_path, "--set-rate-from", "2026-10-05:G01:Z1=2.75")

    files = sorted((tmp_path / "garages").glob("garages_*.jsonl"))
    assert [f.name for f in files] == [f"garages_202610{d:02d}.jsonl" for d in range(1, 7)]
    assert [rate_of_g01_z1(f) for f in files] == [2.0, 2.0, 2.0, 2.0, 2.75, 2.75]
