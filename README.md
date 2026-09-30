# NYC 311 Service Request Data Pipeline

An end-to-end data engineering pipeline built on PySpark, Apache Hive,
PostgreSQL, AWS S3, Jenkins, and Tableau. It ingests a full year of NYC 311
service request data, validates and cleans it, builds a partitioned Hive
warehouse, curates a relational analytical layer in PostgreSQL, publishes an
interactive Tableau dashboard, and runs on an automated CI pipeline — with a
working S3-backed cloud migration on top of the local setup.

**Live dashboard:** https://public.tableau.com/app/profile/sohail.ali.jafar.ali7660/viz/NYC311ServiceRequestsDashboard2023/NYC311OperationsDashboard

Every component runs on free-tier or local resources. No paid AWS service is
required to reproduce this project.

## What this pipeline does

Starting from ~3.2 million raw NYC 311 service requests for 2023, the
pipeline:

1. Downloads and validates the raw data, rejecting malformed records with a
   documented reason for each rejection
2. Cleans, deduplicates, and enriches it in PySpark — standardizing
   categorical fields, deriving resolution-time metrics, and joining
   reference dimensions (agency, complaint category, borough)
3. Writes the result as partitioned Parquet, then registers it as an
   external Hive table with monthly and daily summary tables built via
   HiveQL, including a window-function query ranking each borough's top
   complaint category by month
4. Loads curated summaries into PostgreSQL, with constraints, indexes, and a
   reporting view
5. Publishes a live, cross-filtering Tableau dashboard from that data
6. Validates all of the above with an automated test suite, run on every
   change by a Jenkins pipeline pulling directly from this repository
7. Mirrors the curated dataset to AWS S3 via a least-privilege IAM user,
   demonstrating a small, real cloud migration rather than a local-only
   claim

The row count — **3,220,409** — is checked and matches at every single
stage: Spark output, Hive table, Postgres load, S3 upload.

## Architecture

### Local

```text
Raw CSV (NYC Open Data)
        │
   PySpark (clean, dedupe, enrich)
        │
   Partitioned Parquet
        │
   Apache Hive (external table + summary tables)
        │
   PostgreSQL (relational layer: tables, indexes, view)
        │
   Tableau (published, interactive dashboard)
```

### Cloud (S3-backed)

```text
Raw CSV / Reference tables
        │
   AWS S3 (nyc311-pipeline-sohail-2026)
        │
   PySpark (reads/writes via s3a://, IAM-scoped credentials)
        │
   Curated Parquet in S3, validated against local output
```

Compute stays local (Spark running on this machine); storage moved to S3.
This is a deliberate, cost-conscious scope — a full EMR/cluster migration
was intentionally left out to avoid burning cloud credits on always-on
compute for a project this size. That tradeoff is documented, not hidden.

## Tech stack

| Layer | Technology |
|---|---|
| Processing | Apache Spark (PySpark) |
| Warehouse | Apache Hive (Spark's embedded metastore) |
| Relational store | PostgreSQL |
| Cloud storage | AWS S3, via `hadoop-aws` / `aws-java-sdk-bundle` |
| BI / dashboard | Tableau Public |
| Testing | pytest (19 tests: unit, regression, data quality) |
| CI/CD | Jenkins, pipeline sourced from this repo |
| Version control | Git / GitHub |

## Repository structure

```text
nyc311-data-pipeline/
├── src/
│   ├── acquisition/       raw data download
│   ├── ingestion/         schema validation, ingestion logging
│   ├── reference/         dimension table builder
│   ├── transformation/    Spark cleaning logic (tested, pure functions)
│   ├── hive/              Hive table DDL, summary tables, sample queries
│   ├── postgres/          Postgres load script + helpers (tested)
│   ├── aws/               S3 connectivity test + upload script
│   └── utilities/         shared config helpers
├── sql/postgres/          schema DDL (tables, indexes, view)
├── tests/                 pytest suite — unit, regression, data quality
├── Jenkinsfile            CI pipeline definition
├── pytest.ini
├── data/                  local data (git-ignored — see Setup)
├── docs/                  architecture and usage docs (in progress)
└── README.md
```

## What's been tested and validated

- **19 automated tests**, run locally and in Jenkins: 11 unit/regression
  tests against hand-crafted data, 8 data quality tests against the real
  3.2M-row output (row count, null checks, valid partitions, no negative
  resolution times)
- Every stage of the pipeline independently re-validates the row count
  against the previous stage — this caught two real bugs during
  development (see below)

## Bugs found and fixed along the way

Worth calling out explicitly, since a project that "just worked" the first
time usually means the testing wasn't thorough enough:

- **Hive partition discovery silently returned 0 rows** on Windows —
  `MSCK REPAIR TABLE` wasn't registering partitions correctly; fixed with
  `ALTER TABLE ... RECOVER PARTITIONS` and an explicit absolute `file:///`
  location URI instead of a relative path.
- **`NaN` vs `NULL` corrupting a Postgres aggregate** — two rows with no
  closed requests produced a Spark `NULL` average, which became a literal
  Pandas `NaN`, which `psycopg2` inserted as the *string* `"NaN"` instead of
  SQL `NULL`. This silently broke `AVG()` in a reporting view. Fixed in the
  load script and locked down with a regression test.
- **Spark's default Python worker looked for `python3`**, which doesn't
  exist on Windows — fixed by pointing `PYSPARK_PYTHON` at the venv's
  actual interpreter.

## Setup

```bash
git clone https://github.com/sohailalij/nyc311-data-pipeline.git
cd nyc311-data-pipeline
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

Raw data, reference CSVs, and Parquet output are not committed (see
`.gitignore`) — they're either regenerable from the scripts in `src/`, or
downloadable directly from NYC Open Data. Postgres and AWS credentials are
never stored in files; both are read from environment variables at runtime
(`PG_PASSWORD`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).

Run the pipeline stages in order:

```bash
python src/acquisition/download_311_data.py
python src/reference/build_reference_tables.py
python src/ingestion/validate_and_ingest.py
python src/transformation/clean_311_data.py
python src/hive/create_hive_tables.py
python src/hive/build_summary_tables.py
$env:PG_PASSWORD = "..."; python src/postgres/load_to_postgres.py
```

Run the test suite:

```bash
pytest tests/ -v
```

## Cost

Everything here runs on free-tier or local resources. The only cloud
service used is a small S3 bucket, accessed through a least-privilege IAM
user with no reach outside that one bucket. No compute runs continuously in
the cloud.

## Status

All core pipeline stages are complete and validated end to end. Remaining
work is documentation: architecture write-up and end-user notes.

## License

Add a license of your choice before making broader use of this repository.