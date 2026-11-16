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
- Fact-check (2026-10-10): Correct. One refinement: a CSV reader can skip *converting* unneeded columns but must still scan every byte to find delimiters and line breaks.
- Row order (mine, 2026-10-12): data needs to be ordered or else each row group's min/max value will be messed up, forcing duckdb to read everything.
- Fact-check (2026-10-12): Right idea, two refinements. (1) Unsorted min/max values aren't wrong, just wide: each row group spans nearly the whole range, so no group can be excluded. What matters is clustering on the *column you filter by*; sorting by started_at helps time filters, not station filters. (2) "Read everything" means every row group of the *needed columns*, because column skipping still works.
- References:
  - Apache Parquet, File Format: https://parquet.apache.org/docs/file-format/ (row groups, column chunks, file metadata)
  - Apache Parquet, Metadata: https://parquet.apache.org/docs/file-format/metadata/
  - DuckDB, Parquet overview: https://duckdb.org/docs/current/data/parquet/overview.html ("projection pushdown into the Parquet file itself", "filter pushdown into the Parquet reader", min/max statistics)
- Misconceptions:
- Quiz history:

### Partitioning
- Added: 2026-10-10
- Summary (mine): pending (open: how a query gets partition pruning when it filters on started_at)
- References:
  - DuckDB, Hive Partitioning (see "Filter Pushdown"): https://duckdb.org/docs/current/data/partitioning/hive_partitioning.html
- Misconceptions:
  - 2026-10-07: Monthly partitions give more data quantity (then: more data granularity) than yearly partitions → Partitioning is physical file layout. Every row keeps its own timestamps whatever the partitioning, and the analysis grain is chosen at query time. Partition size trades off how much a filtered query can skip, file count and size, and how much must be rewritten when something changes.
  - 2026-10-10: A revised month can be handled by selectively updating cells → Parquet files are immutable (self-corrected 2026-10-12). A revision rewrites the whole file, so the partition is the unit of rewrite.
- Quiz history:

### Storage formats vs. model inputs
- Added: 2026-11-11
- Summary (mine, 2026-11-16): The file format stops mattering once the data is read into memory. Thereafter, the libraries interact with the data structures in memory rather than the file format (e.g. numpy arrays).
- Fact-check (2026-11-16): Right for sklearn, statsmodels, PyTorch and PyMC. Two gaps. (1) "Read into memory" covers two steps: the reader builds its own structure (a polars DataFrame is Arrow-backed; a pandas DataFrame is numpy-backed by default), and that structure is then converted into the array the model receives. The file format stops mattering after the first step, but types can still change in the second: an integer column with nulls becomes float with NaN (design.md F20). (2) It doesn't hold where the model library reads the file itself, as `tf.data` does with CSV and CmdStanPy does with a JSON data file. There the format has to suit the model library.
- References:
  - pandas, `read_parquet` (see `dtype_backend`): https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html
  - Apache Arrow, pandas integration (nullable types): https://arrow.apache.org/docs/python/pandas.html
  - Polars, `Series.to_numpy` ("Integer Series containing nulls will be cast to a float type with nan"): https://docs.pola.rs/api/python/stable/reference/series/api/polars.Series.to_numpy.html
  - TensorFlow, `tf.data.experimental.make_csv_dataset` (reads CSV files itself): https://www.tensorflow.org/api_docs/python/tf/data/experimental/make_csv_dataset
  - CmdStanPy API, `CmdStanModel.sample(data=...)` (a dict, or a path to a JSON or Rdump file): https://mc-stan.org/cmdstanpy/api.html
  - Polars, Coming from pandas ("Polars represents data in memory according to the Arrow memory spec while pandas by default represents data in memory with NumPy arrays"): https://docs.pola.rs/user-guide/migration/pandas/
- Misconceptions:
  - 2026-11-11: Parquet's portability with ML packages (sklearn, statsmodels, TensorFlow, PyTorch) is a reason to choose it (design.md D1) → Those packages take in-memory arrays or DataFrames. The file format only has to suit the reader library (pyarrow, pandas, polars, DuckDB). Exceptions that read files themselves: TensorFlow's `tf.data` (CSV) and CmdStan (JSON data files). Partly self-corrected in design.md F6.
- Quiz history:

### Dataframe engines (pandas, polars, DuckDB)
- Added: 2026-11-16
- Summary (mine): pending (design.md F26)
- References:
  - Polars, Coming from pandas ("polars != pandas"; Arrow memory, expressions, no index, strict types, lazy and eager evaluation): https://docs.pola.rs/user-guide/migration/pandas/
  - Polars, Lazy optimizations (predicate pushdown, projection pushdown, slice pushdown, among others): https://docs.pola.rs/user-guide/lazy/optimizations/
  - DuckDB, Parquet overview (projection and filter pushdown): https://duckdb.org/docs/current/data/parquet/overview.html
- Misconceptions:
  - 2026-11-16: Polars is a natural extension of pandas, while DuckDB is a different approach altogether (design.md D8) → Polars isn't built on pandas, and its docs say "polars != pandas". It stores data in Arrow format rather than numpy, has no index and is strict about types. Its lazy engine pushes filters and column selection down into the scan, as DuckDB does.
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
