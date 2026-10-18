"""Tests for the job-ready batch runner (run_batch.py)."""

import json
import sys
from pathlib import Path

import pytest
import run_batch as rb

GENERATOR = Path(__file__).resolve().parent.parent / "generator"


def test_the_next_batch_number_follows_the_highest_existing_one(tmp_path):
    assert rb.next_batch_number(tmp_path / "missing") == 1
    for name in ("batch_0001_a.jsonl", "batch_0006_b.jsonl", "batch_0002_replay.jsonl", "notes.txt"):
        (tmp_path / name).touch()
    assert rb.next_batch_number(tmp_path) == 7


def run_batch(monkeypatch, root, *extra):
    monkeypatch.setattr(sys, "argv", ["run_batch.py", "--root", str(root), "--sessions", "30", *extra])
    rb.main()


def entries_of(path):
    events = []
    for line in path.read_text().splitlines():
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return [e for e in events if e["event_type"] == "entry"]


def test_each_run_makes_the_next_batch_and_keeps_its_state_in_the_landing_root(tmp_path, monkeypatch, capsys):
    run_batch(monkeypatch, tmp_path)
    run_batch(monkeypatch, tmp_path)
    out = capsys.readouterr().out

    assert "Next batch: 1, date 2026-10-01, seed 1" in out
    assert "Next batch: 2, date 2026-10-02, seed 2" in out
    files = sorted(p.name for p in (tmp_path / "parking_events").glob("*.jsonl"))
    assert [f[:11] for f in files] == ["batch_0001_", "batch_0002_"]
    assert (tmp_path / "_generator_state").is_dir()


def test_the_same_series_gives_the_same_files(tmp_path, monkeypatch):
    for name in ("a", "b"):
        for _ in range(3):
            run_batch(monkeypatch, tmp_path / name)

    def contents(name):
        return [p.read_text() for p in sorted((tmp_path / name / "parking_events").glob("*.jsonl"))]

    assert contents("a") == contents("b")


def test_the_schema_changes_at_batch_five_and_six_through_the_runner(tmp_path, monkeypatch):
    for _ in range(6):
        run_batch(monkeypatch, tmp_path)
    files = sorted((tmp_path / "parking_events").glob("*.jsonl"))
    shapes = [
        (any("vehicle_type" in e for e in entries_of(f)), any("entry_gate" in e for e in entries_of(f)))
        for f in files
    ]
    assert shapes == [(False, False)] * 4 + [(True, False), (True, True)]


def test_the_code_folder_can_be_given_explicitly(tmp_path, monkeypatch):
    run_batch(monkeypatch, tmp_path, "--code-dir", str(GENERATOR))
    assert len(list((tmp_path / "parking_events").glob("*.jsonl"))) == 1


def test_the_code_folder_lookup_does_not_mistake_pytest_style_abbreviations(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["pytest", "--co", "-q"])
    assert rb.code_dir() == GENERATOR


def test_a_missing_code_folder_is_a_clear_error_without_file_variable(monkeypatch):
    monkeypatch.delitem(rb.code_dir.__globals__, "__file__")
    monkeypatch.setattr(sys, "argv", ["run_batch.py"])
    with pytest.raises(SystemExit, match="--code-dir is required"):
        rb.code_dir()
