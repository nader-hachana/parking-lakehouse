-- Datasets for the parking dashboard. One query per dataset, separated by a line starting with "-- dataset:".
-- Times are UTC.

-- dataset: freshness
-- Hours since the newest ingestion, evaluated when the dashboard is viewed. Stale after 26 hours (daily run plus tolerance).
SELECT
  MAX(_ingested_at) AS last_ingested_at,
  ROUND((unix_timestamp(current_timestamp()) - unix_timestamp(MAX(_ingested_at))) / 3600, 1) AS hours_since_last_ingestion,
  CASE WHEN (unix_timestamp(current_timestamp()) - unix_timestamp(MAX(_ingested_at))) / 3600 > 26
       THEN 'STALE' ELSE 'OK' END AS freshness
FROM parking.bronze.raw_events;

-- dataset: last_batch
SELECT batch, ingested_at, raw_lines, rejected_rows, reject_share, missing_plate
FROM parking.gold.data_quality_by_batch
ORDER BY batch DESC
LIMIT 1;

-- dataset: quality_by_batch
-- One row per delivered file. Rejected rows by reason for a stacked bar, reject_share for a line.
SELECT batch, raw_lines, rejected_rows, reject_share,
       rejected_valid_json, rejected_has_event_id, rejected_known_event_type, rejected_valid_amount,
       missing_plate
FROM parking.gold.data_quality_by_batch
ORDER BY batch;

-- dataset: revenue_daily
SELECT revenue_date, garage_id, revenue_eur, payments
FROM parking.gold.revenue_daily
ORDER BY revenue_date, garage_id;

-- dataset: parking_duration_daily
SELECT entry_date, garage_id, avg_duration_minutes, complete_sessions, open_sessions, invalid_sessions
FROM parking.gold.parking_duration_daily
ORDER BY entry_date, garage_id;

-- dataset: zone_occupancy_hourly
-- Use garage_id and zone_id as dashboard filters.
SELECT hour_ts, garage_id, zone_id, cars_present, capacity, occupancy_pct
FROM parking.gold.zone_occupancy_hourly
ORDER BY hour_ts;

-- dataset: sessions_by_status
-- How complete the data is: open sessions are waiting for an exit, no_entry sessions lost their entry.
SELECT status, COUNT(*) AS sessions
FROM parking.silver.sessions
GROUP BY status
ORDER BY sessions DESC;
