# Learning Log

Maintained by Claude under the rules in CLAUDE.md. Summaries are in my own words, fact-checked against the listed references. Say "quiz me" to be tested on anything here.

## Causal inference

_No entries yet._

## Bayesian methods

_No entries yet._

## Data engineering

### Parquet columnar storage
- Added: 2026-10-10
- Summary (mine): Parquet uses columnar storage, the irrelevant columns in the query are skipped. CSV differs in that all columns have to be loaded in for every query. By recording the minimum and maximum value for each row group, the row groups that do not fit within the filter conditions are ignored instead of unnecessarily parsed.
- Fact-check (2026-10-10): Correct. One refinement: a CSV reader can skip *converting* unneeded columns but must still scan every byte to find delimiters and line breaks. Open question: how row order affects min/max skipping.
- References:
  - Apache Parquet, File Format: https://parquet.apache.org/docs/file-format/ (row groups, column chunks, file metadata)
  - Apache Parquet, Metadata: https://parquet.apache.org/docs/file-format/metadata/
  - DuckDB, Parquet overview: https://duckdb.org/docs/current/data/parquet/overview.html ("projection pushdown into the Parquet file itself", "filter pushdown into the Parquet reader", min/max statistics)
- Misconceptions:
- Quiz history:

### Partitioning
- Added: 2026-10-10
- Summary (mine): pending (revising: what a revised month costs given how Parquet files are laid out; partition column vs. filter column)
- References:
  - DuckDB, Hive Partitioning (see "Filter Pushdown"): https://duckdb.org/docs/current/data/partitioning/hive_partitioning.html
- Misconceptions:
  - 2026-10-07: Monthly partitions give more data quantity (then: more data granularity) than yearly partitions → Partitioning is physical file layout. Every row keeps its own timestamps whatever the partitioning, and the analysis grain is chosen at query time. Partition size trades off how much a filtered query can skip, file count and size, and how much must be rewritten when something changes.
- Quiz history:

<!--
Entry template: one per concept, under its track. If a misconception comes up
before I've written a summary, create the entry with "Summary (mine): pending".

### <Concept>
- Added: YYYY-MM-DD
- Summary (mine):
- References:
- Misconceptions:
  - YYYY-MM-DD: <what I believed> → <what's actually true>
- Quiz history:
  - YYYY-MM-DD: concept | application, ✓ / partial / ✗, <one-line note>
-->
