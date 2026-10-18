# Databricks notebook source
# Parking lakehouse pipeline: bronze (raw files), silver (validated, deduplicated events,
# quarantine table, zone history). Gold is added in Phase 4.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "parking"
LANDING = "/Volumes/parking/landing/events"
RAW_EVENTS = f"{CATALOG}.bronze.raw_events"
RAW_GARAGES = f"{CATALOG}.bronze.raw_garages"

# COMMAND ----------

# Quality rules, defined once. The same dictionary drives the expectations and the
# quarantine table, so "dropped" and "quarantined" cannot drift apart.
DROP_RULES = {
    "valid_json": "_corrupt_record IS NULL",
    "has_event_id": "event_id IS NOT NULL",
    "known_event_type": "event_type IN ('entry', 'payment', 'exit')",
    "valid_amount": "event_type <> 'payment' OR (amount_eur IS NOT NULL AND amount_eur > 0)",
}

# Warn only: occupancy, duration and revenue do not depend on the plate or the vehicle type.
WARN_RULES = {
    "has_plate": "plate IS NOT NULL",
    "known_vehicle_type": "vehicle_type IS NULL OR vehicle_type IN ('car', 'ev', 'motorcycle')",
}


def first_failed_rule(rules: dict):
    """Name of the first rule a row breaks, or null if it passes all of them.
    A rule that evaluates to null counts as failed, as in the expectations."""
    reason = F.lit(None).cast("string")
    for name, rule in reversed(list(rules.items())):
        failed = ~F.coalesce(F.expr(rule), F.lit(False))
        reason = F.when(failed, F.lit(name)).otherwise(reason)
    return reason

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze
# MAGIC Raw files, no cleaning. `_corrupt_record` keeps the raw text of lines that are not
# MAGIC valid JSON, so silver can count and quarantine them instead of losing them.

# COMMAND ----------

@dp.table(name=RAW_EVENTS, comment="Raw parking events from the landing volume.")
def raw_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "_corrupt_record STRING")
        # Trusted internal producer: new fields become columns. The stream fails once
        # when it sees a new field and the retry picks up the new schema. Silver selects
        # its columns explicitly, so a new bronze column does not reach silver by itself.
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{LANDING}/parking_events")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(name=RAW_GARAGES, comment="Raw garage and zone snapshots from the landing volume.")
def raw_garages():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaHints", "capacity INT, hourly_rate_eur DOUBLE, snapshot_ts TIMESTAMP")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{LANDING}/garages")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: events

# COMMAND ----------


@dp.table(
    name=f"{CATALOG}.silver.events_valid",
    comment="Typed events that passed the quality rules. May still contain duplicates.",
)
@dp.expect_all_or_drop(DROP_RULES)
@dp.expect_all(WARN_RULES)
def events_valid():
    raw = spark.readStream.table(RAW_EVENTS)
    # vehicle_type only appears in the source from batch 5. Until bronze has the column,
    # silver gets nulls instead of failing on an unresolved column.
    vehicle_type = (
        F.col("vehicle_type")
        if "vehicle_type" in raw.columns
        else F.lit(None).cast("string")
    ).alias("vehicle_type")
    return raw.select(
        "event_id",
        "event_type",
        F.col("event_ts").cast("timestamp").alias("event_ts"),
        "session_id",
        "garage_id",
        "zone_id",
        "plate",
        "amount_eur",
        "payment_method",
        vehicle_type,
        "_ingested_at",
        "_source_file",
        "_corrupt_record",
    )


@dp.table(
    name=f"{CATALOG}.silver.events_rejected",
    comment="Events dropped from silver, with the first rule they broke.",
)
def events_rejected():
    return (
        spark.readStream.table(RAW_EVENTS)
        .withColumn("reject_reason", first_failed_rule(DROP_RULES))
        .where(F.col("reject_reason").isNotNull())
    )


# One row per event_id. A retry or a replayed file repeats the same event_id,
# so keeping the latest version by ingestion time removes the duplicates.
dp.create_streaming_table(
    name=f"{CATALOG}.silver.events",
    comment="Deduplicated events, one row per event_id.",
)

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.events",
    source=f"{CATALOG}.silver.events_valid",
    keys=["event_id"],
    sequence_by="_ingested_at",
    stored_as_scd_type=1,
    except_column_list=["_corrupt_record"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: zone dimension with history
# MAGIC SCD Type 2. A new version is created when zone_name, capacity or hourly_rate_eur
# MAGIC changes. `snapshot_ts` is the sequence column and is left out of the target columns,
# MAGIC otherwise every snapshot would look like a change.

# COMMAND ----------

dp.create_streaming_table(
    name=f"{CATALOG}.silver.dim_zone",
    comment="Garage zones with history. __END_AT is null for the current version.",
)

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.dim_zone",
    source=RAW_GARAGES,
    keys=["garage_id", "zone_id"],
    sequence_by="snapshot_ts",
    stored_as_scd_type=2,
    except_column_list=["snapshot_ts", "_source_file", "_ingested_at", "_rescued_data"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: sessions
# MAGIC One row per session_id, built from its entry, payment and exit events. A materialized
# MAGIC view, because a late exit must change a session that was already computed. Checks that
# MAGIC need more than one event (exit before entry, missing entry or exit) live here.

# COMMAND ----------

EVENTS = f"{CATALOG}.silver.events"
SESSIONS = f"{CATALOG}.silver.sessions"
DIM_ZONE = f"{CATALOG}.silver.dim_zone"


@dp.materialized_view(
    name=SESSIONS,
    comment="One row per parking session with status and duration. Duration only for complete sessions.",
)
def sessions():
    e = F.col("event_type")
    s = (
        spark.read.table(EVENTS)
        .groupBy("session_id")
        .agg(
            F.min(F.when(e == "entry", F.col("event_ts"))).alias("entry_ts"),
            F.max(F.when(e == "exit", F.col("event_ts"))).alias("exit_ts"),
            F.max(F.when(e == "payment", F.col("amount_eur"))).alias("amount_eur"),
            F.max("garage_id").alias("garage_id"),
            F.max("zone_id").alias("zone_id"),
            F.max("plate").alias("plate"),
        )
        .withColumn(
            "status",
            F.when(F.col("entry_ts").isNull(), "no_entry")
            .when(F.col("exit_ts").isNull(), "open")
            .when(F.col("exit_ts") < F.col("entry_ts"), "invalid_order")
            .otherwise("complete"),
        )
    )
    return s.withColumn(
        "duration_minutes",
        F.when(
            F.col("status") == "complete",
            (F.unix_timestamp("exit_ts") - F.unix_timestamp("entry_ts")) / 60,
        ),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold
# MAGIC Business tables for the dashboard. All materialized views, so late data corrects
# MAGIC numbers that were already computed.

# COMMAND ----------


@dp.materialized_view(
    name=f"{CATALOG}.gold.revenue_daily",
    comment="Revenue and number of payments per garage and payment date.",
)
def revenue_daily():
    return (
        spark.read.table(EVENTS)
        .where(F.col("event_type") == "payment")
        .groupBy("garage_id", F.to_date("event_ts").alias("revenue_date"))
        .agg(
            F.round(F.sum("amount_eur"), 2).alias("revenue_eur"),
            F.count("*").alias("payments"),
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.parking_duration_daily",
    comment="Average parking duration per garage and entry date, with session status counts.",
)
def parking_duration_daily():
    status = F.col("status")
    return (
        spark.read.table(SESSIONS)
        .where(F.col("entry_ts").isNotNull())
        .groupBy("garage_id", F.to_date("entry_ts").alias("entry_date"))
        .agg(
            F.round(F.avg("duration_minutes"), 1).alias("avg_duration_minutes"),
            F.sum(F.when(status == "complete", 1).otherwise(0)).alias("complete_sessions"),
            F.sum(F.when(status == "open", 1).otherwise(0)).alias("open_sessions"),
            F.sum(F.when(status == "invalid_order", 1).otherwise(0)).alias("invalid_sessions"),
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.zone_occupancy_hourly",
    comment="Cars present per zone and hour (complete sessions), with capacity valid at that hour.",
)
def zone_occupancy_hourly():
    complete = spark.read.table(SESSIONS).where(F.col("status") == "complete")
    present = (
        complete.withColumn(
            "hour_ts",
            F.explode(
                F.sequence(
                    F.date_trunc("hour", "entry_ts"),
                    F.date_trunc("hour", "exit_ts"),
                    F.expr("interval 1 hour"),
                )
            ),
        )
        .groupBy("garage_id", "zone_id", "hour_ts")
        .agg(F.countDistinct("session_id").alias("cars_present"))
        .alias("o")
    )
    zones = spark.read.table(DIM_ZONE).alias("z")
    # Point-in-time join: the zone version that was valid at that hour.
    valid_at_hour = (
        (F.col("o.garage_id") == F.col("z.garage_id"))
        & (F.col("o.zone_id") == F.col("z.zone_id"))
        & (F.col("o.hour_ts") >= F.col("z.__START_AT"))
        & (F.col("z.__END_AT").isNull() | (F.col("o.hour_ts") < F.col("z.__END_AT")))
    )
    return present.join(zones, valid_at_hour, "left").select(
        F.col("o.garage_id"),
        F.col("o.zone_id"),
        F.col("o.hour_ts"),
        F.col("o.cars_present"),
        F.col("z.capacity"),
        F.round(F.col("o.cars_present") / F.col("z.capacity") * 100, 1).alias("occupancy_pct"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: data quality per batch
# MAGIC One row per delivered file: when it was ingested, how many lines it had and how many
# MAGIC were rejected and why. The numbers come from the tables themselves, so they can be
# MAGIC queried and charted, and old batches do not disappear like job run outputs do.

# COMMAND ----------


def batch_quality(raw, rejected, valid):
    """One row per source file. `raw` is the bronze table, `rejected` and `valid` the silver ones."""
    reason_columns = [f"rejected_{reason}" for reason in DROP_RULES]

    lines = raw.groupBy("_source_file").agg(
        F.min("_ingested_at").alias("ingested_at"),
        F.count("*").alias("raw_lines"),
    )
    rejects = rejected.groupBy("_source_file").agg(
        F.count("*").alias("rejected_rows"),
        *[
            F.sum(F.when(F.col("reject_reason") == reason, 1).otherwise(0)).alias(f"rejected_{reason}")
            for reason in DROP_RULES
        ],
    )
    warned = valid.groupBy("_source_file").agg(
        F.sum(F.when(F.col("plate").isNull(), 1).otherwise(0)).alias("missing_plate")
    )

    return (
        lines.join(rejects, "_source_file", "left")
        .join(warned, "_source_file", "left")
        .na.fill(0, ["rejected_rows", "missing_plate", *reason_columns])
        .withColumn("reject_share", F.round(F.col("rejected_rows") / F.col("raw_lines"), 4))
        .withColumn("batch", F.regexp_extract("_source_file", r"batch_(\d+)_", 1).cast("int"))
        .select(
            "batch",
            "ingested_at",
            "raw_lines",
            "rejected_rows",
            "reject_share",
            *reason_columns,
            "missing_plate",
            "_source_file",
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.data_quality_by_batch",
    comment="Lines, rejected rows (by reason) and missing plates per delivered file.",
)
def data_quality_by_batch():
    return batch_quality(
        spark.read.table(RAW_EVENTS),
        spark.read.table(f"{CATALOG}.silver.events_rejected"),
        spark.read.table(f"{CATALOG}.silver.events_valid"),
    )
