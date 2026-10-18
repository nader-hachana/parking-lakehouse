# Parking Lakehouse

[![deploy](https://github.com/nader-hachana/parking-lakehouse/actions/workflows/deploy.yml/badge.svg)](https://github.com/nader-hachana/parking-lakehouse/actions/workflows/deploy.yml)

A data pipeline on Databricks that turns raw, messy parking garage events into clean, tested tables and a dashboard. Everything runs on a schedule, and the job, the pipeline and the dashboard are deployed from this repository by CI/CD.

I built it to learn Databricks hands-on, coming from PostgreSQL, ClickHouse and PySpark. I picked parking because I already know the domain, so I could focus on the platform instead of the business logic.

What this project shows:

- Incremental ingestion with Auto Loader and a bronze, silver, gold design in Unity Catalog
- Data quality as code: drop and warn rules, a quarantine table, deduplication, SCD Type 2 history
- A scheduled job with retries and a final data check
- Tests with a local Spark session, and CI/CD that deploys the job, pipeline and dashboard from this repository

## Quick start

Without a Databricks account you can run the data generators locally. You need [uv](https://docs.astral.sh/uv/) (it installs Python 3.12 for you), and Java 11 to 17 only if you want the Spark tests.

```bash
git clone https://github.com/nader-hachana/parking-lakehouse.git
cd parking-lakehouse
uv sync
uv run pytest
uv run python generator/run_batch.py --root data/local/landing --sessions 100
```

The pipeline, the job and the dashboard need a Databricks workspace (the free edition is enough): see [Setup and deployment](#setup-and-deployment). The bundle looks up a SQL warehouse called "Serverless Starter Warehouse", so change `lookup` in `databricks.yml` if yours is named differently. The deploy workflow needs the `DATABRICKS_HOST` and `DATABRICKS_TOKEN` secrets, so it will not run on a fork.

## Architecture

![Architecture](architecture.png)

- **Sources (simulated):** two generators write JSON files into a volume. Events (entry, payment, exit) arrive in batches, zone attributes (capacity, hourly rate) arrive as daily snapshots.
- **Bronze:** the files as they arrived, loaded incrementally with Auto Loader. Nothing is cleaned, and broken lines are kept with their raw text.
- **Silver:** typed events that passed the quality rules (drop or warn), a quarantine table with the reason for every dropped record, one row per `event_id`, a zone dimension with history (SCD Type 2), and one row per parking session.
- **Gold:** revenue per garage and day, average parking duration, occupancy per zone and hour, and data quality per delivered file.
- **Delivery:** a Databricks job orchestrates generation, pipeline and a final data check. The job, the pipeline and the dashboard are one bundle, deployed by GitHub Actions after a merge.

The generated data is messy on purpose: duplicates (inside a file and across files), late events, missing fields, invalid values, broken lines, and two new fields that appear over time. That is what gives the cleaning steps something to do.

## Verified result

The data is simulated, with known problems injected on purpose (duplicates, late events, broken lines, invalid amounts, schema changes). Six runs of the job from an empty workspace, repeated three times (by hand, through the CLI and through the bundle), gave the same numbers every time:

- 1,815 raw lines in, 13 rejected with their reason (10 broken lines, 3 invalid amounts), 1,770 unique events after deduplication
- 600 sessions, and 7 zone history rows (one zone changed its rate on 2026-10-05, so it has two versions)
- Gold revenue equals the silver payments (6,720.1), so nothing was lost or counted twice

## How it runs

- **generate_batch** (`generator/run_batch.py`) writes the next batch of events. It works out the batch number and date itself and keeps its state in the volume.
- **generate_garages** (`generator/run_garages.py`) writes the next daily snapshot of the zones, including a scheduled rate change.
- **run_pipeline** ingests the new files and refreshes bronze, silver and gold.
- **check_results** (`checks/check_results.py`) fails the run when the numbers are clearly wrong, for example an empty table or more than 5 % of raw lines rejected.

The job has a daily schedule at 06:00 Vienna time, paused by default.

## Setup and deployment

One-time, in a Databricks workspace (the SQL editor):

```sql
CREATE CATALOG IF NOT EXISTS parking;
CREATE SCHEMA IF NOT EXISTS parking.landing;
CREATE SCHEMA IF NOT EXISTS parking.bronze;
CREATE SCHEMA IF NOT EXISTS parking.silver;
CREATE SCHEMA IF NOT EXISTS parking.gold;
CREATE VOLUME IF NOT EXISTS parking.landing.events;
```

Then, with the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/) authenticated (`databricks auth login --host <workspace url> --profile parking`):

```bash
databricks bundle validate --profile parking
databricks bundle deploy --profile parking
databricks bundle run parking_daily --profile parking
```

`deploy` uploads the code and creates or updates the job, the pipeline and the dashboard. Run the last command once per batch to follow the data through the layers, or switch the schedule on with `--var schedule_pause_status=UNPAUSED`. To start over, empty the landing folders in the volume and deploy a fresh bundle (after `databricks bundle destroy`, and drop the pipeline's tables, which are not always removed with it).

The generators also run on their own with [uv](https://docs.astral.sh/uv/):

```bash
uv run python generator/run_batch.py --root data/local/landing --sessions 100
uv run python generator/run_garages.py --root data/local/landing --set-rate-from 2026-10-05:G01:Z1=2.75
```

## Observability

- **Pipeline health query** (`queries/pipeline_health.sql`): "how many bad records did we drop yesterday, and is today's data fresh?". Dropped records come from `gold.data_quality_by_batch` (one row per delivered file: lines, rejected rows by reason, rows kept with a missing plate), freshness from the newest `_ingested_at` in bronze (stale after 26 hours, because a daily run is expected). Freshness uses the arrival time: the generated events carry dates that follow the batch number, not the clock.
- **Dashboard** (`dashboards/parking_dashboard.lvdash.json`, deployed by the bundle): a page for pipeline health (hours since the last ingestion, freshness status, records dropped yesterday, reject share of the last delivery, rejected rows by reason per batch) and a page for the business (revenue and average parking duration per garage and day, occupancy by hour with garage and zone filters, sessions by status). The datasets are in `queries/dashboard_datasets.sql`, and the SQL warehouse is found by name at deploy time.

## Tests

```bash
uv run pytest
uv run ruff check .
```

- **Generator:** the data it makes (ordered sessions, unique event ids), every injected problem, the schema changes, and the state handling (a failed write leaves the state untouched).
- **Pipeline design:** the real pipeline file is imported with a fake `pyspark.pipelines`, so the tests check the tables, the CDC keys and the shared rule dictionary.
- **Rules and data quality logic:** run on sample rows with a small local Spark session (PySpark 3.5, needs Java 11 to 17, skipped without Java).

## CI/CD

- **Every pull request:** `ruff` and `pytest`, plus `bundle validate` and `bundle plan`, so a broken bundle is caught before the merge.
- **After a merge to `main`:** the same checks, then a plan that stops if it would create, recreate or delete anything, then `bundle deploy`. The job is not run and the schedule stays paused.
- **`main` is protected:** pull request only, with `lint-and-test` and `validate-bundle` green. Direct pushes are rejected, for the owner too.
- **Credentials:** `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are Actions secrets. The token is a personal access token that expires after 30 days: create a new one and run `gh secret set DATABRICKS_TOKEN`.

## Repository layout

```
generator/        simulates the source systems (events and zone snapshots)
pipelines/        the Lakeflow Declarative Pipeline: bronze, silver and gold
checks/           data checks run as the last task of the job
queries/          saved SQL: the pipeline health query and the dashboard datasets
dashboards/       the dashboard definition (AI/BI dashboard as a .lvdash.json file)
resources/        the job, the pipeline and the dashboard as bundle resources
databricks.yml    the bundle: what gets deployed and how
architecture.drawio / .png   the architecture diagram (source and export)
tests/            unit tests for the generator and the pipeline design
.github/          the CI, bundle validation and deploy workflows
```

## Limitations and what I would do next

- **Free Edition:** small data volumes, serverless compute only and capped (running notebooks or queries during a job run can trigger a "limit for serverless compute" error, which the pipeline retries by itself), one active pipeline, managed storage only, one workspace (so a single `dev` target).
- **Simulated data:** event dates follow the batch number, not the clock, so freshness is measured on arrival time. Occupancy only counts complete sessions, so it is understated until a late exit arrives.
- **Volumes cannot append to an existing file**, so the generator rewrites its state files as whole files.
- **No alerts yet:** the dashboard shows freshness and the reject share, but nothing notifies anyone. The first thing I would add is a freshness alert, because a paused or deleted job fails silently.
- **The end to end result is checked by hand** (the six run series). I would automate it as a nightly run on a dev target.
- **Deploy hardening:** a service principal with OAuth instead of a token, a `prod` target in a second workspace, and pinned versions for the Databricks CLI action.
