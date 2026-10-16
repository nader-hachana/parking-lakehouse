# Databricks notebook source
# MAGIC %md
# MAGIC # Check results
# MAGIC Last task of the daily job. Fails the job when the numbers are clearly wrong, so a
# MAGIC silent problem becomes a visible failed run.

# COMMAND ----------

import json

MAX_REJECT_SHARE = 0.05  # more than 5 % of raw lines rejected is not normal for this feed

raw_lines = spark.table("parking.bronze.raw_events").count()
rejected = spark.table("parking.silver.events_rejected").count()
events = spark.table("parking.silver.events").count()
sessions = spark.table("parking.silver.sessions").count()
last_ingest = spark.sql("SELECT MAX(_ingested_at) FROM parking.bronze.raw_events").first()[0]

reject_share = rejected / raw_lines if raw_lines else 1.0
metrics = {
    "raw_lines": raw_lines,
    "rejected": rejected,
    "reject_share": round(reject_share, 4),
    "events": events,
    "sessions": sessions,
    "last_ingested_at": str(last_ingest),
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

if problems:
    raise AssertionError("Data checks failed: " + "; ".join(problems) + " | " + json.dumps(metrics))

dbutils.notebook.exit(json.dumps(metrics))
