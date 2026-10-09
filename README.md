# Parking Lakehouse

A small data project on Databricks that turns raw parking garage events into clean, tested tables and a dashboard.

I built it to learn Databricks hands-on, coming from a background in PostgreSQL, ClickHouse and PySpark. I picked parking because I already know the domain, so I could focus on the platform instead of the business logic.

> Work in progress. The sections below describe the plan and get updated as each phase is finished.

## What it does

A Python script generates parking events (cars entering, leaving, paying) and writes them as JSON files. Databricks picks them up and processes them in three layers:

- **Bronze:** the raw events, loaded as they arrive with Auto Loader
- **Silver:** cleaned and deduplicated events, plus a garage/zone table that keeps its history (SCD Type 2)
- **Gold:** ready-to-use numbers: occupancy per zone and hour, revenue per garage per day, average parking duration

A Databricks SQL dashboard sits on top of the gold tables.

The generated data is messy on purpose: duplicates, late events, missing fields, and a new field that appears halfway through. That is what makes the cleaning steps worth doing.

## Tech

- Databricks Free Edition (serverless)
- Unity Catalog and Volumes
- Auto Loader, Lakeflow Declarative Pipelines, Delta Lake
- Databricks Jobs, Asset Bundles
- Python, SQL, PySpark
- GitHub Actions

## Project status

| Phase | What | Status |
|---|---|---|
| 0 | Setup: workspace, CLI, catalog, schemas, landing volume | Done |
| 1 | Event generator | Done |
| 2 | Bronze with Auto Loader | To do |
| 3 | Silver with Lakeflow Declarative Pipelines | To do |
| 4 | Gold tables and performance | To do |
| 5 | Orchestration with Jobs | To do |
| 6 | Tests and CI/CD with Asset Bundles | To do |
| 7 | Observability and dashboard | To do |
| 8 | Documentation | To do |

## Layout in Unity Catalog

```
parking                catalog
  landing              raw JSON files (volume: events)
  bronze               raw ingested tables
  silver               cleaned tables
  gold                 business-level tables
```

## How to run it

The event generator runs locally with [uv](https://docs.astral.sh/uv/):

```bash
uv run python generator/generate_events.py --sessions 100 --seed 1 --batch 1 --date 2026-10-01
```

Each run writes a new JSON Lines file to `data/local/parking_events/`. Use a different `--batch`, `--seed` and `--date` for every run to see late and duplicate events show up across files. Uploading to the Databricks volume and the pipeline come in later phases.

## Limitations

- Free Edition has usage limits, so the data volumes are small.
- Storage is managed by Databricks, since Free Edition does not allow custom storage locations.
