# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion: parking events
# MAGIC
# MAGIC Reads new JSON Lines files from the landing volume with Auto Loader and appends them
# MAGIC to `parking.bronze.events`. Bronze keeps the data raw: no cleaning, no deduplication.
# MAGIC Quality rules belong in silver.

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE = "/Volumes/parking/landing/events/parking_events"
STATE = "/Volumes/parking/bronze/checkpoints/events"
TARGET = "parking.bronze.events"
BAD_RECORDS = f"{STATE}/bad_records"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read
# MAGIC
# MAGIC `cloudFiles` is Auto Loader. The schema location is where it stores the schema it infers
# MAGIC and its evolution history. Without `inferColumnTypes`, JSON columns are all strings.
# MAGIC No schema is provided, so the evolution mode defaults to `addNewColumns`.
# MAGIC
# MAGIC `badRecordsPath` is an experiment. The first run (with neither option) turned broken
# MAGIC lines into all-null rows and lost their text. A `_corrupt_record` column fixed that
# MAGIC (see git history). Here the broken lines go to files under `BAD_RECORDS` instead.

# COMMAND ----------

raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", f"{STATE}/schema")
    # Lines that are not valid JSON are recorded here instead of becoming
    # all-null rows. Non-transactional: a retry can write the same record twice.
    .option("badRecordsPath", BAD_RECORDS)
    .load(SOURCE)
    # Processing time, to measure lateness against event_ts later.
    .withColumn("_ingested_at", F.current_timestamp())
    # Which file each row came from, to trace duplicates and bad rows.
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write
# MAGIC
# MAGIC The checkpoint is what makes a rerun safe: files already processed are skipped.
# MAGIC `availableNow` processes everything new and then stops, which suits a scheduled job.

# COMMAND ----------

query = (
    raw.writeStream.option("checkpointLocation", f"{STATE}/checkpoint")
    # Auto Loader evolves its own schema, but the Delta table does not follow
    # unless the write allows it. Without this, a new source field fails every retry.
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET)
)
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check

# COMMAND ----------

display(spark.sql(f"SELECT _source_file, COUNT(*) AS row_count FROM {TARGET} GROUP BY _source_file ORDER BY _source_file"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bad records
# MAGIC
# MAGIC What was written under `BAD_RECORDS`. The folder may not exist if nothing was bad.

# COMMAND ----------

bad = spark.read.option("recursiveFileLookup", "true").json(BAD_RECORDS)
bad.printSchema()
display(bad)
