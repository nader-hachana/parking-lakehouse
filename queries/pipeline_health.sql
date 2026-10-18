-- How many bad records did we drop yesterday, and is today's data fresh?
--
-- Times are UTC. "Yesterday" is the ingestion date, because a corrupt line has no event time.
-- A daily run is expected, so more than 26 hours without a new ingestion (24 hours plus
-- some tolerance) counts as stale. Needs the gold.data_quality_by_batch view.

WITH yesterday AS (
  SELECT
    COALESCE(SUM(raw_lines), 0)                  AS lines_ingested,
    COALESCE(SUM(rejected_rows), 0)              AS records_dropped,
    COALESCE(SUM(rejected_valid_json), 0)        AS corrupt_lines,
    COALESCE(SUM(rejected_has_event_id), 0)      AS missing_event_id,
    COALESCE(SUM(rejected_known_event_type), 0)  AS unknown_event_type,
    COALESCE(SUM(rejected_valid_amount), 0)      AS invalid_amount,
    COALESCE(SUM(missing_plate), 0)              AS kept_with_missing_plate
  FROM parking.gold.data_quality_by_batch
  WHERE DATE(ingested_at) = current_date() - INTERVAL 1 DAY
),
freshness AS (
  SELECT
    MAX(_ingested_at) AS last_ingested_at,
    ROUND((unix_timestamp(current_timestamp()) - unix_timestamp(MAX(_ingested_at))) / 3600, 1)
      AS hours_since_last_ingestion
  FROM parking.bronze.raw_events
)
SELECT
  y.*,
  f.last_ingested_at,
  f.hours_since_last_ingestion,
  CASE WHEN f.hours_since_last_ingestion > 26 THEN 'STALE' ELSE 'OK' END AS freshness
FROM yesterday y CROSS JOIN freshness f;
