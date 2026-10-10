# stat-relearning

Learning causal inference, Bayesian methods and data engineering on [Bluebikes](https://s3.amazonaws.com/hubway-data/index.html) trip data.

## Setup

```sh
py -3.13 -m venv .venv
.venv/Scripts/python -m pip install -e ".[dev]"
```

## Raw ingest

Downloads the monthly trip files and writes one Parquet partition per source file under `data/` (git-ignored).

```sh
.venv/Scripts/python -m bluebikes.ingest --start 2023-10            # through the latest month in the bucket
.venv/Scripts/python -m bluebikes.ingest --start 2024-01 --end 2024-06
```

Re-running only re-ingests months whose source file changed. A month whose inferred schema differs from the
existing partitions is rejected and logged; the run exits non-zero.

```
data/
  raw/                         original downloads (+ .etag sidecars)
  parquet/trips/month=YYYY-MM/data.parquet
  manifest.json                what was ingested, from which file version
```

Query all months together:

```sql
SELECT * FROM read_parquet('data/parquet/trips/**/*.parquet', hive_partitioning = true);
```

## Tests

```sh
.venv/Scripts/python -m pytest              # offline
.venv/Scripts/python -m pytest -m network   # also checks the live bucket
```
