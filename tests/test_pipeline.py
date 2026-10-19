"""Tests for the pipeline file, without Databricks.

The real file is imported with a fake `pyspark.pipelines` that records what the file declares
(tables, expectations, CDC flows). That tests the design of the pipeline, and the SQL quality
rules and the transformations are run on sample rows with a small local Spark session.
"""

import importlib.util
import types
from pathlib import Path
from unittest import mock

import pyspark
import pytest

PIPELINE_FILE = Path(__file__).resolve().parent.parent / "pipelines" / "parking_pipeline.py"


class FakePipelines:
    """Stands in for `from pyspark import pipelines as dp` and records every declaration."""

    def __init__(self):
        self.datasets = {}
        self.streaming_tables = []
        self.flows = []

    def _dataset(self, kind):
        def factory(*_args, **kwargs):
            def decorator(fn):
                name = kwargs.get("name", fn.__name__)
                self.datasets[name] = {"kind": kind, "expectations": getattr(fn, "_expectations", [])}
                return fn

            return decorator

        return factory

    @staticmethod
    def _expectation(kind):
        def factory(rules):
            def decorator(fn):
                fn.__dict__.setdefault("_expectations", []).append((kind, rules))
                return fn

            return decorator

        return factory

    def module(self):
        fake = types.ModuleType("pyspark.pipelines")
        fake.table = self._dataset("table")
        fake.materialized_view = self._dataset("materialized_view")
        fake.expect_all_or_drop = self._expectation("expect_all_or_drop")
        fake.expect_all = self._expectation("expect_all")
        fake.create_streaming_table = lambda **kwargs: self.streaming_tables.append(kwargs["name"])
        fake.create_auto_cdc_flow = lambda **kwargs: self.flows.append(kwargs)
        return fake


@pytest.fixture(scope="module")
def pipeline():
    fake = FakePipelines()
    with (
        mock.patch.dict("sys.modules", {"pyspark.pipelines": fake.module()}),
        mock.patch.object(pyspark, "pipelines", fake.module(), create=True),
    ):
        spec = importlib.util.spec_from_file_location("parking_pipeline", PIPELINE_FILE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module, fake


# --- the declared design ----------------------------------------------------


def test_the_pipeline_declares_every_layer(pipeline):
    _, fake = pipeline
    kinds = {name: d["kind"] for name, d in fake.datasets.items()}
    assert kinds == {
        "parking.bronze.raw_events": "table",
        "parking.bronze.raw_garages": "table",
        "parking.silver.events_valid": "table",
        "parking.silver.events_rejected": "table",
        "parking.silver.sessions": "materialized_view",
        "parking.gold.revenue_daily": "materialized_view",
        "parking.gold.parking_duration_daily": "materialized_view",
        "parking.gold.zone_occupancy_hourly": "materialized_view",
        "parking.gold.data_quality_by_batch": "materialized_view",
    }
    assert fake.streaming_tables == ["parking.silver.events", "parking.silver.dim_zone"]


def test_events_valid_uses_the_shared_rules_as_drop_and_warn(pipeline):
    module, fake = pipeline
    expectations = fake.datasets["parking.silver.events_valid"]["expectations"]
    assert ("expect_all_or_drop", module.DROP_RULES) in expectations
    assert ("expect_all", module.WARN_RULES) in expectations


def flow_for(fake, target):
    (flow,) = [f for f in fake.flows if f["target"] == target]
    return flow


def test_events_are_deduplicated_by_event_id_keeping_the_latest(pipeline):
    flow = flow_for(pipeline[1], "parking.silver.events")
    assert flow["source"] == "parking.silver.events_valid"
    assert flow["keys"] == ["event_id"]
    assert flow["sequence_by"] == "_ingested_at"
    assert flow["stored_as_scd_type"] == 1


def test_zones_keep_history_and_the_sequence_column_is_not_tracked(pipeline):
    flow = flow_for(pipeline[1], "parking.silver.dim_zone")
    assert flow["keys"] == ["garage_id", "zone_id"]
    assert flow["sequence_by"] == "snapshot_ts"
    assert flow["stored_as_scd_type"] == 2
    # Otherwise every snapshot would look like a change and create a new version.
    assert "snapshot_ts" in flow["except_column_list"]


# --- the quality rules, evaluated by Spark ----------------------------------

SCHEMA = (
    "event_id string, event_type string, amount_eur double, plate string, "
    "vehicle_type string, _corrupt_record string"
)
ROWS = {
    "good_entry": ("e1", "entry", None, "W-1", None, None),
    "good_payment": ("e2", "payment", 12.5, "W-1", None, None),
    "corrupt_line": (None, None, None, None, None, '{"event_id": "x", "ev'),
    "no_event_id": (None, "entry", None, "W-1", None, None),
    "unknown_type": ("e3", "refund", 5.0, "W-1", None, None),
    "negative_amount": ("e4", "payment", -3.0, "W-1", None, None),
    "null_amount": ("e5", "payment", None, "W-1", None, None),
    "no_plate": ("e6", "entry", None, None, None, None),
    "odd_vehicle": ("e7", "entry", None, "W-1", "truck", None),
}


@pytest.fixture
def sample(spark):
    names = list(ROWS)
    df = spark.createDataFrame([(n, *ROWS[n]) for n in names], "name string, " + SCHEMA)
    return df


def reasons(pipeline_module, df, rules):
    rows = df.withColumn("reason", pipeline_module.first_failed_rule(rules)).select("name", "reason").collect()
    return {r["name"]: r["reason"] for r in rows}


def test_each_bad_row_is_dropped_for_the_first_rule_it_breaks(pipeline, sample):
    assert reasons(pipeline[0], sample, pipeline[0].DROP_RULES) == {
        "good_entry": None,
        "good_payment": None,
        "corrupt_line": "valid_json",
        "no_event_id": "has_event_id",
        "unknown_type": "known_event_type",
        "negative_amount": "valid_amount",
        "null_amount": "valid_amount",
        "no_plate": None,
        "odd_vehicle": None,
    }


def test_a_missing_plate_and_an_unknown_vehicle_type_only_warn(pipeline, sample):
    assert reasons(pipeline[0], sample, pipeline[0].WARN_RULES) == {
        "good_entry": None,
        "good_payment": None,
        "corrupt_line": "has_plate",
        "no_event_id": None,
        "unknown_type": None,
        "negative_amount": None,
        "null_amount": None,
        "no_plate": "has_plate",
        "odd_vehicle": "known_vehicle_type",
    }


def test_the_quarantine_table_holds_exactly_the_rows_the_drop_rules_remove(pipeline, sample):
    """One rule dictionary drives both the expectations and the quarantine reason."""
    from pyspark.sql import functions as F

    module = pipeline[0]
    passes_all = F.lit(True)
    for rule in module.DROP_RULES.values():
        passes_all = passes_all & F.coalesce(F.expr(rule), F.lit(False))

    kept = {r["name"] for r in sample.where(passes_all).collect()}
    rejected = {r["name"] for r in sample.where(module.first_failed_rule(module.DROP_RULES).isNotNull()).collect()}
    assert kept.isdisjoint(rejected)
    assert kept | rejected == set(ROWS)


# --- data quality per batch -------------------------------------------------


def test_batch_quality_counts_lines_rejects_and_warnings_per_file(pipeline, spark):
    from datetime import datetime

    first, second = "/Volumes/x/parking_events/batch_0001_a.jsonl", "/Volumes/x/parking_events/batch_0002_b.jsonl"
    raw = spark.createDataFrame(
        [(first, datetime(2026, 10, 1, 6, 0))] * 9
        + [(first, datetime(2026, 10, 1, 6, 1))]
        + [(second, datetime(2026, 10, 2, 6, 0))] * 5,
        "_source_file string, _ingested_at timestamp",
    )
    rejected = spark.createDataFrame(
        [(first, "valid_json"), (first, "valid_amount")], "_source_file string, reject_reason string"
    )
    valid = spark.createDataFrame(
        [(first, "W-1"), (first, None), (first, None), (second, None), (second, "W-2")],
        "_source_file string, plate string",
    )

    rows = {r["batch"]: r.asDict() for r in pipeline[0].batch_quality(raw, rejected, valid).collect()}

    assert set(rows) == {1, 2}
    assert rows[1]["raw_lines"] == 10
    assert rows[1]["rejected_rows"] == 2
    assert rows[1]["reject_share"] == 0.2
    assert (rows[1]["rejected_valid_json"], rows[1]["rejected_valid_amount"]) == (1, 1)
    assert rows[1]["missing_plate"] == 2
    assert rows[1]["ingested_at"] == datetime(2026, 10, 1, 6, 0)
    # A file without any rejection gets zeros, not nulls, and its own warning count.
    assert rows[2]["raw_lines"] == 5
    assert (rows[2]["rejected_rows"], rows[2]["reject_share"], rows[2]["rejected_valid_json"]) == (0, 0.0, 0)
    assert rows[2]["missing_plate"] == 1


def test_batch_quality_has_one_column_per_drop_rule(pipeline, spark):
    module = pipeline[0]
    raw = spark.createDataFrame([("batch_0001_x", None)], "_source_file string, _ingested_at timestamp")
    rejected = spark.createDataFrame([], "_source_file string, reject_reason string")
    valid = spark.createDataFrame([], "_source_file string, plate string")
    columns = module.batch_quality(raw, rejected, valid).columns
    for rule in module.DROP_RULES:
        assert f"rejected_{rule}" in columns



# --- sessions and gold ------------------------------------------------------


def ts(day, hour, minute=0):
    from datetime import datetime

    return datetime(2026, 10, day, hour, minute)


EVENT_SCHEMA = (
    "event_id string, event_type string, event_ts timestamp, session_id string, "
    "garage_id string, zone_id string, plate string, amount_eur double"
)


@pytest.fixture
def events(spark):
    rows = [
        # s1: complete, 90 minutes, paid 4.5
        ("a1", "entry", ts(1, 8, 0), "s1", "G01", "Z1", "W-1", None),
        ("a2", "payment", ts(1, 9, 25), "s1", "G01", "Z1", "W-1", 4.5),
        ("a3", "exit", ts(1, 9, 30), "s1", "G01", "Z1", "W-1", None),
        # s2: still open (no exit yet)
        ("b1", "entry", ts(1, 10, 0), "s2", "G01", "Z1", "W-2", None),
        # s3: exit without entry
        ("c1", "exit", ts(1, 11, 0), "s3", "G02", "Z1", "W-3", None),
        # s4: exit before entry
        ("d1", "entry", ts(1, 12, 0), "s4", "G02", "Z1", "W-4", None),
        ("d2", "exit", ts(1, 11, 0), "s4", "G02", "Z1", "W-4", None),
        # s5: complete, 60 minutes, paid 3.0 the next day
        ("e1", "entry", ts(1, 22, 30), "s5", "G02", "Z1", "W-5", None),
        ("e2", "payment", ts(2, 0, 10), "s5", "G02", "Z1", "W-5", 3.0),
        ("e3", "exit", ts(2, 0, 30), "s5", "G02", "Z1", "W-5", None),
    ]
    return spark.createDataFrame(rows, EVENT_SCHEMA)


def by_key(df, *keys):
    return {tuple(r[k] for k in keys) if len(keys) > 1 else r[keys[0]]: r.asDict() for r in df.collect()}


def test_a_session_gets_its_status_from_its_entry_and_exit(pipeline, events):
    sessions = by_key(pipeline[0].build_sessions(events), "session_id")
    assert {sid: s["status"] for sid, s in sessions.items()} == {
        "s1": "complete",
        "s2": "open",
        "s3": "no_entry",
        "s4": "invalid_order",
        "s5": "complete",
    }


def test_duration_and_amount_are_only_set_where_they_make_sense(pipeline, events):
    sessions = by_key(pipeline[0].build_sessions(events), "session_id")
    assert sessions["s1"]["duration_minutes"] == 90
    assert sessions["s1"]["amount_eur"] == 4.5
    assert sessions["s5"]["duration_minutes"] == 120
    for incomplete in ("s2", "s3", "s4"):
        assert sessions[incomplete]["duration_minutes"] is None
    assert sessions["s2"]["amount_eur"] is None


def test_revenue_is_summed_per_garage_and_payment_date(pipeline, events):
    revenue = by_key(pipeline[0].build_revenue_daily(events), "garage_id", "revenue_date")
    from datetime import date

    assert revenue[("G01", date(2026, 10, 1))]["revenue_eur"] == 4.5
    assert revenue[("G02", date(2026, 10, 2))]["revenue_eur"] == 3.0
    assert revenue[("G02", date(2026, 10, 2))]["payments"] == 1
    assert len(revenue) == 2


def test_gold_revenue_equals_the_payments_in_silver(pipeline, events):
    from pyspark.sql import functions as F

    paid = events.where(F.col("event_type") == "payment").agg(F.sum("amount_eur")).first()[0]
    gold = pipeline[0].build_revenue_daily(events).agg(F.sum("revenue_eur")).first()[0]
    assert gold == paid


def test_parking_duration_counts_sessions_by_status_per_entry_date(pipeline, spark, events):
    module = pipeline[0]
    sessions = module.build_sessions(events)
    daily = by_key(module.build_parking_duration_daily(sessions), "garage_id", "entry_date")
    from datetime import date

    g01 = daily[("G01", date(2026, 10, 1))]
    assert (g01["complete_sessions"], g01["open_sessions"], g01["invalid_sessions"]) == (1, 1, 0)
    assert g01["avg_duration_minutes"] == 90.0
    g02 = daily[("G02", date(2026, 10, 1))]
    # s3 has no entry and is left out, s4 is invalid, s5 is complete.
    assert (g02["complete_sessions"], g02["open_sessions"], g02["invalid_sessions"]) == (1, 0, 1)
    assert g02["avg_duration_minutes"] == 120.0


ZONE_SCHEMA = "garage_id string, zone_id string, capacity int, __START_AT timestamp, __END_AT timestamp"


def occupancy(module, spark, events, zones):
    sessions = module.build_sessions(events)
    return by_key(
        module.build_zone_occupancy(sessions, spark.createDataFrame(zones, ZONE_SCHEMA)),
        "garage_id",
        "zone_id",
        "hour_ts",
    )


def test_a_complete_session_counts_in_every_hour_it_touches(pipeline, spark, events):
    zones = [("G01", "Z1", 40, ts(1, 0), None), ("G02", "Z1", 50, ts(1, 0), None)]
    rows = occupancy(pipeline[0], spark, events, zones)
    # s1 is parked 08:00 to 09:30, so it is present in the 08:00 and 09:00 hours.
    assert sorted(h for (g, z, h) in rows if g == "G01") == [ts(1, 8), ts(1, 9)]
    assert rows[("G01", "Z1", ts(1, 8))]["cars_present"] == 1
    assert rows[("G01", "Z1", ts(1, 8))]["occupancy_pct"] == 2.5
    # s5 crosses midnight: 22:30 to 00:30 covers three hours.
    assert sorted(h for (g, z, h) in rows if g == "G02") == [ts(1, 22), ts(1, 23), ts(2, 0)]


def test_open_and_invalid_sessions_are_not_counted_as_present(pipeline, spark, events):
    zones = [("G01", "Z1", 40, ts(1, 0), None), ("G02", "Z1", 50, ts(1, 0), None)]
    rows = occupancy(pipeline[0], spark, events, zones)
    assert ("G01", "Z1", ts(1, 10)) not in rows
    assert ("G02", "Z1", ts(1, 11)) not in rows
    assert ("G02", "Z1", ts(1, 12)) not in rows


def test_capacity_comes_from_the_zone_version_valid_at_that_hour(pipeline, spark, events):
    zones = [
        ("G01", "Z1", 40, ts(1, 0), ts(1, 9)),
        ("G01", "Z1", 80, ts(1, 9), None),
        ("G02", "Z1", 50, ts(1, 0), None),
    ]
    rows = occupancy(pipeline[0], spark, events, zones)
    assert rows[("G01", "Z1", ts(1, 8))]["capacity"] == 40
    assert rows[("G01", "Z1", ts(1, 9))]["capacity"] == 80


def test_hours_before_the_first_zone_snapshot_have_no_capacity(pipeline, spark, events):
    zones = [("G01", "Z1", 40, ts(1, 9), None), ("G02", "Z1", 50, ts(1, 0), None)]
    row = occupancy(pipeline[0], spark, events, zones)[("G01", "Z1", ts(1, 8))]
    assert row["capacity"] is None
    assert row["occupancy_pct"] is None
