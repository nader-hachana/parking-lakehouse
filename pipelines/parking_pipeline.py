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
    return spark.readStream.table(RAW_EVENTS).select(
        "event_id",
        "event_type",
        F.col("event_ts").cast("timestamp").alias("event_ts"),
        "session_id",
        "garage_id",
        "zone_id",
        "plate",
        "amount_eur",
        "payment_method",
        "vehicle_type",
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
