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

The generated data is messy on purpose: duplicates, late events, missing fields, invalid values, broken lines, and new fields that appear over time. That is what makes the cleaning steps worth doing.

## Tech

- Databricks Free Edition (serverless)
- Unity Catalog and Volumes
- Auto Loader, Lakeflow Declarative Pipelines, Delta Lake
- Databricks Jobs
- Python, SQL, PySpark
- Asset Bundles and GitHub Actions (planned, Phase 6)

## Project status

| Phase | What | Status |
|---|---|---|
| 0 | Setup: workspace, CLI, catalog, schemas, landing volume | Done |
| 1 | Event generator | Done |
| 2 | Bronze with Auto Loader | Done |
| 3 | Silver with Lakeflow Declarative Pipelines | Done |
| 4 | Gold tables and performance | Done |
| 5 | Orchestration with Jobs | Done |
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

## Repository layout

```
generator/    simulates the source systems (events and zone snapshots)
pipelines/    the Lakeflow Declarative Pipeline: bronze, silver and gold
checks/       data checks run as the last task of the job
```

## How it runs

A Databricks job runs these tasks in order:

```
generate_batch  ----\
                      >----  run_pipeline  ---->  check_results
generate_garages ---/
```

- **generate_batch** (`generator/run_batch.py`) writes the next batch of parking events to the landing volume. It works out the batch number and date itself and keeps its state in the volume.
- **generate_garages** (`generator/run_garages.py`) writes the next daily snapshot of the garage zones, including a scheduled rate change.
- **run_pipeline** ingests the new files and refreshes bronze, silver (events, rejected events, sessions, zone history) and the gold tables (revenue, parking duration, zone occupancy).
- **check_results** (`checks/check_results.py`) fails the run when the numbers are clearly wrong, for example an empty table or more than 5 % of raw lines rejected.

The generators inject problems on purpose (duplicates, late events, missing and invalid values, broken lines, two schema changes), so the pipeline has real work to do.

The job has a daily schedule trigger. It is currently created by hand with the Databricks CLI and is not defined in this repository. Defining it as code (Asset Bundles) is part of Phase 6.

## Running the generator locally

The generator also runs on its own with [uv](https://docs.astral.sh/uv/):

```bash
uv run python generator/run_batch.py --root data/local/landing --sessions 100
uv run python generator/run_garages.py --root data/local/landing --set-rate-from 2026-10-05:G01:Z1=2.75
```

Each call writes the next file into `data/local/landing/`, so running it several times produces a series of batches.

## Limitations

- Free Edition has usage limits, so the data volumes are small. Serverless compute is capped: running notebooks or queries while the job runs can trigger a "limit for serverless compute" error, which the pipeline retries by itself.
- Unity Catalog volumes cannot append to an existing file, so the generator rewrites its state files as whole files.
- Storage is managed by Databricks, since Free Edition does not allow custom storage locations.
