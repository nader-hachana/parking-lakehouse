# Databricks notebook source
# MAGIC %md
# MAGIC # Check results
# MAGIC Last task of the daily job. Fails the job when the numbers are clearly wrong, so a
# MAGIC silent problem becomes a visible failed run.

# COMMAND ----------

import json

MAX_REJECT_SHARE = 0.05  # more than 5 % of raw lines rejected is not normal for this feed
MAX_STALE_HOURS = 26  # a daily run is expected, so a day plus some slack

raw_lines = spark.table("parking.bronze.raw_events").count()
rejected = spark.table("parking.silver.events_rejected").count()
events = spark.table("parking.silver.events").count()
sessions = spark.table("parking.silver.sessions").count()

distinct_event_ids = spark.sql("SELECT COUNT(DISTINCT event_id) FROM parking.silver.events").first()[0]
silver_payments = spark.sql(
    "SELECT COALESCE(SUM(amount_eur), 0) FROM parking.silver.events WHERE event_type = 'payment'"
).first()[0]
gold_revenue = spark.sql("SELECT COALESCE(SUM(revenue_eur), 0) FROM parking.gold.revenue_daily").first()[0]
hours_since_ingest = spark.sql(
    "SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(MAX(_ingested_at))) / 3600 "
    "FROM parking.bronze.raw_events"
).first()[0]

reject_share = rejected / raw_lines if raw_lines else 1.0
metrics = {
    "raw_lines": raw_lines,
    "rejected": rejected,
    "reject_share": round(reject_share, 4),
    "events": events,
    "distinct_event_ids": distinct_event_ids,
    "sessions": sessions,
    "silver_payments_eur": round(silver_payments, 2),
    "gold_revenue_eur": round(gold_revenue, 2),
    "hours_since_last_ingestion": None if hours_since_ingest is None else round(hours_since_ingest, 1),
}
print(json.dumps(metrics, indent=2))

# COMMAND ----------

problems = []
if raw_lines == 0:
    problems.append("bronze is empty")
if events == 0:
    problems.append("silver.events is empty")
if sessions == 0:
    problems.append("silver.sessions is empty")
if reject_share > MAX_REJECT_SHARE:
    problems.append(f"reject share {reject_share:.1%} is above {MAX_REJECT_SHARE:.0%}")
if events != distinct_event_ids:
    problems.append(f"silver.events has {events} rows but {distinct_event_ids} distinct event ids")
if abs(silver_payments - gold_revenue) > 0.005:
    problems.append(f"gold revenue {gold_revenue:.2f} differs from silver payments {silver_payments:.2f}")
if hours_since_ingest is None or hours_since_ingest > MAX_STALE_HOURS:
    problems.append(f"no ingestion in the last {MAX_STALE_HOURS} hours")

if problems:
    raise AssertionError("Data checks failed: " + "; ".join(problems) + " | " + json.dumps(metrics))

dbutils.notebook.exit(json.dumps(metrics))
