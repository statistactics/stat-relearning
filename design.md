# Monthly Bluebikes ingest: design (draft)

- **Status:** draft, condensed 2026-11-16. Answers from rounds 1–3 are folded into takeaways (§7). Only open questions remain in full (§8).
  - **Decided:** D1 Parquet, D2 metadata comparison, D3 overwrite, D4 trip-level ingest, D5 weekly, D6 all history, D8 DuckDB.
  - **Open:** D7, because your F14 answer points two ways. Questions F9 and F22–F26 are also open, and F24 questions D3.
- **Decision owner:** Caleb. Claude drafts the source facts, options and questions, records your answers and reviews them. Per CLAUDE.md, no option is recommended.
- **How to use this doc:** answer §8 inline.

## 1. Goal

Each month, fetch the newest Bluebikes trip file from the public `hubway-data` bucket (https://s3.amazonaws.com/hubway-data/), plus any earlier month that was re-published, and store the data so Bayesian and causal analyses can read it with known, enforced types. The newest month also provides the actuals that earlier forecasts are scored against.

**Principles**
- **Scripts and systems, not notebooks.** Every pipeline step is a script, run by the scheduler or from the command line. Notebooks are for prototyping only, and nothing in the pipeline depends on one.
- **Medallion layers**, as laid out in §4.

| ID | Requirement |
|----|-------------|
| R1 | Read the monthly trip files from the bucket, zipped or plain CSV. |
| R2 | Store them in a format that suits the reader library. Models take in-memory arrays, not files (Q3, F6, F19). |
| R3 | Enforce types more strictly than CSV can: Parquet (D1) with a declared schema. The layer that enforces it is open (D7). |
| R4 | Run unattended on this laptop, weekly (D5). |
| R5 | Pull the latest month to score forecasts against (Q4; see F24). |
| R6 | Detect a re-publish of any month and rewrite that month (Q5, Q6, D2). |

Not yet known, and only affecting gold: the forecasting target and the causal design (Q1, Q2).

## 2. The source, as checked on 2026-11-02

Checked with anonymous `ListObjectsV2` and `ListObjectVersions` calls, a `HEAD` request, and the headers of the 202303, 202304 and 202609 files.

| Fact | Detail | Why it matters |
|------|--------|----------------|
| Listing size | 150 objects on one page (`IsTruncated=false`; a page holds up to 1,000 keys). | Checking every month for changes costs one request. |
| Coverage | 141 monthly trip files, 201501–202609. 202610 was not published as of 2026-11-02. | F12 ingests all of them. |
| Volume | 202304–202609: 42 files, 751 MB as stored (41 zips plus the 133 MB plain CSV for 202609). The 202606–202608 zips average 21.8 MB. 201501–202303 not yet sized. | Disk use (A7). |
| Publish lag | For data months from 2022 on, 56 of 57 files have a `LastModified` in the following month, between day 1 and day 26 (median: day 5). 7 of the 56 are after the 10th; the latest was 202312, on 2024-01-26. | Sets the warning day (F9). |
| Key names drift | `YYYYMM-hubway-tripdata.zip` → `YYYYMM-bluebikes-tripdata.zip` → `202511-bluebikes-tripdata.csv.zip` → `202609-bluebikes-tripdata.csv` (plain CSV). The zips also contain a `__MACOSX/._*.csv` member. Station lists and the yearly `hubway_Trips_2011…2014` files share the prefix. | Mapping a key to its month needs a rule, not one pattern (A1, F23). |
| Schema break | Up to 202303: `tripduration, starttime, stoptime, start station id, …, bikeid, usertype, postal code`. From 202304: `ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, …, member_casual`. | With F12, both schemas arrive on the first run (F23). |
| Type traps | Station IDs: `386` (202303) vs. `"A32011"` (202304 on). `postal code` values such as `02139` have a leading zero. Timestamp precision: 4 decimals (202303), whole seconds (202304), milliseconds (202609). No time-zone offsets. | A sniffer guesses types from the rows it samples (DuckDB: 20,480 by default), and an integer type turns `02139` into `2139`. |
| **Versioning is off** | `ListObjectVersions` returns exactly 150 entries, all with `VersionId=null`, including 202609 (written 2026-10-05). | An overwrite at the source destroys the previous version. The bucket keeps no old releases (D3). |
| ETags aren't always content hashes | SSE-S3. Single-part uploads have MD5 ETags. Multipart uploads carry a `-N` suffix (202609 has `-8`) and aren't MD5 digests. | Re-uploading identical bytes with a different part size changes the ETag, so D2 re-downloads and rewrites that month. Accepted (F16). |
| Revisions are rare and only partly visible | Among data months from 2022 on, only 202308 was last written more than a month later: at 2023-10-12 05:40:30, two seconds before 202309 was uploaded. Before 2021 there were bulk re-uploads (e.g. 201906–201908, all on 2020-11-17). | `LastModified` shows only the latest write. Every month is checked (Q5), so it doesn't matter which ones get revised. |

## 3. Assumptions

| ID | Assumption | Evidence so far | How to test | If wrong |
|----|------------|-----------------|-------------|----------|
| A1 | A key's `YYYYMM` prefix is the month of the trips inside, with at most one trip file per month. | True for all 141 keys. | Each run: count trip keys per `YYYYMM`; min and max start time per file. | Two keys for one month (e.g. `.zip` and `.csv`) leave it unclear which one counts. |
| A2 | A month's file holds the trips that *started* that month. | Unverified. | Min and max start time per file. | Trips that cross a month boundary appear twice, or not at all. |
| A3 | `ride_id` is unique within and across files. Applies from 202304 only; older months have no `ride_id`. | Unverified. | Row count vs. distinct `ride_id`. | No row key for de-duplication in silver, or for your F5 idea. |
| A4 | Timestamps are Boston wall-clock time (America/New_York), kept that way; DST is handled in silver (Q9, F11). | Not documented anywhere I've found. | Commute peaks near 08:00 and 17:00; durations around DST changes (e.g. 2025-11-02 01:00–02:00). | Hour-of-day features and durations shift. |
| A5 | A revision overwrites the same key instead of adding a new one. | 202308. | A1's per-month key count. | A revision looks like a duplicate month. |
| A7 | Volume stays laptop-sized. | 751 MB for 202304–202609; earlier months unsized. No disk limit (F10). | Log bytes and rows per run. | Download and rewrite costs change D1 and D2. |
| A8 | It runs on this Windows 11 laptop, which can be asleep or off at the scheduled time. | Confirmed (Q10). | — | — |
| A9 | Nothing in the repo constrains this design. | The reverted ingest and `pyproject.toml` were removed (2026-11-04). | — | — |
| A10 | The bucket stays public and anonymous, and your use is covered by Bluebikes' data license. | I haven't read the license. | Read it, linked from https://bluebikes.com/system-data. | — |
| A11 | Every layer stores one unit per source month, so a revision rewrites one month per layer (Q6). | Not contradicted so far. | — | Tell me if you meant something else. |

A6 and A12 were superseded by Q5 and F12.

## 4. Layers (medallion structure)

Each layer is written by a script, never a notebook. The raw/bronze and silver rows follow your F14 clarification and F21. F22 asks you to confirm them.

| Layer | Holds | Rules | Status |
|-------|-------|-------|--------|
| Raw = bronze | Each month's file exactly as published: zips stay zipped, plain CSV stays plain (F10, F21). Every month from 201501 (F12). | Latest version only (Q12). Nothing stops a month from going live (Q8). | Pending F22. |
| Silver | Typed, validated trips in Parquet (D1), one partition per month. | Cast to a schema declared in the repo; failed casts go to a reject table (Q7). Also: schema reconciliation, validation, de-duplication, derived columns, empty vs. NULL (F4) and DST (F11). | Rules not yet defined. Which months it covers: F23. |
| Gold | Model-ready datasets: hierarchical forecasting inputs and causal panels. | Built once the forecasting and causal work are scoped (Q1, Q2). Integer columns with gaps are settled here (F20). | Deferred. |

DuckDB's `read_csv` decompresses gzip and zstd but not zip, so reading a zipped month means extracting the CSV member first (Python's `zipfile`) and skipping the `__MACOSX` entry. This is incidental code; no decision is needed.

Pointers: https://docs.databricks.com/aws/en/lakehouse/medallion, https://www.databricks.com/glossary/medallion-architecture. The term comes from Databricks, but the pattern doesn't depend on their platform.

## 5. Design decisions

Option tables and documentation links are in Appendix A.

### D1. Stored format: **Parquet** (decided)
- **Why (F15):** Parquet stores types and is more efficient than CSV, and you want experience with a format you don't know yet. Portability with ML packages is no longer a reason (F6).
- **Why the others lose:** CSV (A) loses on types and efficiency. The DuckDB database file (C) hasn't been argued against yet (F25).

### D2. Change detection: **compare S3 metadata** (decided)
- Every run lists the bucket and compares each month's ETag, `LastModified` and size with a local manifest (Q5, Q6). If any of them differs, the run re-downloads the month and rewrites it in every layer.
- Content hashing was dropped to keep things simple (F16). Accepted cost: an identical re-upload triggers a needless re-download and rewrite (§2, ETags).

### D3. Revised months: **overwrite** (decided; F24 questions it)
- One version per month in every layer; raw files stay until overwritten (Q4, Q12).
- Accepted: a reported score can change after a revision, and the old score can't be reproduced. A backtest re-run later trains on data that the original forecast never saw.
- Your forecasts live in their own table (F17b), which D3 doesn't cover.
- F17(a) may reverse Q4. See F24.

### D4. Grain: **trip-level, format-agnostic ingest** (decided)
Panels are built in gold once the causal work is scoped (Q2). Questions for the gold design: the unit and the period, and how a station-day with no trips differs from a station that didn't exist yet. Search term: balanced vs. unbalanced panel.

### D5. Schedule: **weekly** (decided)
- Task Scheduler with `StartWhenAvailable` runs a missed task once the laptop wakes. Every run checks every month, so any run catches up on one that was missed. Logs go to a file.
- **Your reason (F18):** "More opportunities for catch-up? It's arbitrary."
- **Review:** late arrivals don't matter to you (Q10), so extra catch-up buys little. Weekly is fine to keep, but it's also what causes the early-month false alarms in F9.

### D6. History: **every month available** (decided)
F12 replaces both the Q13 backward walk and the hardcoded 202304 start (old A12). Bronze takes every month and silver reconciles the schemas. What counts as "available", and which months silver covers: F23.

### D7. Where types are enforced: **open**
F14 picked B (declared schema, cast in bronze), then said Parquet and schema enforcement should only start at silver. Both can't hold. See F22.

### D8. Transform engine: **DuckDB** (decided)
- **Your reason:** more exposure; polars "seems like a natural extension/improvement on pandas whereas DuckDB seems like a different approach altogether."
- **Consequences:** transforms are versioned `.sql` files plus a thin Python runner, so two languages. Data is converted to numpy at the model boundary (`.df()`, `.fetchnumpy()`), where F20 applies.
- **Review:** half of this holds. The interface does differ (SQL vs. a Python dataframe API), but polars isn't an extension of pandas. See F26.

Out of scope for now: the module split, function signatures and tests. We draft those together once D7 is settled.

## 6. Source checks to repeat each run

These only log; none of them stops a month from going live (Q8). Mark the ones you want and drop the rest.

- [ ] Count trip keys per `YYYYMM`; flag more than one (A1, A5).
- [ ] Log when a month's column names or types differ from the previous month's (F2).
- [ ] Min and max of the start time per file (A2).
- [ ] Row count vs. distinct `ride_id`, from 202304 on (A3).
- [ ] Rejected-row count per month, in whichever layer casts (Q7, D7).

## 7. Takeaways from rounds 1–3

**Scope**
- The ingest delivers trip-level data with no panel structure. The forecasting target (likely hierarchical, with short and long horizons) and the causal design aren't fixed. They only shape gold (Q1, Q2, D4).
- Model libraries take in-memory arrays or DataFrames, not files, so the file format only has to suit the reader library (Q3, F6, F19). The exceptions are TensorFlow's `tf.data`, which reads CSV itself, and CmdStanPy, which accepts a JSON data file. F19 is fact-checked in LEARNING_LOG.

**Revisions and scoring**
- Every run checks every month and only needs to know *whether* one changed (Q5, Q6). S3 metadata decides; there's no hashing (F16).
- One version per month: a revision overwrites (Q12, D3).
- Scores follow the latest release (Q4), though F17(a) suggests otherwise (F24).
- Forecasts are stored in their own table on the laptop (F17b).

**Types and quality**
- Types come from a schema declared in the repo, not inferred from a sample (F14).
- A row that fails a cast goes to a reject table, and nothing stops a month from going live (Q7, Q8).
- A schema change means different column names or types; order is ignored. It is logged, not blocked (F2).
- Timestamps stay as published, in local wall-clock time. The repeated DST hour is handled in silver (Q9, F11).
- Deferred until the data is in hand:
  - Empty field vs. NULL, in silver (F4).
  - Reject-table cleanup after a revision (F5). Test the `ride_id` idea against three cases: a rejected row that's missing from the revised file, a row whose `ride_id` is unreadable, and pre-202304 months, which have no `ride_id`.
  - One shared dataset or one per schema version (F13). F12 makes this due on the first run; see F23.
  - Integer columns with gaps, in gold (F20).

**History and storage**
- Every available month goes in, from 201501. There's no backward walk and no hardcoded start (F12).
- Raw keeps each file exactly as published (F10, F21). Zipping saves disk, not memory. There's no disk limit.
- Parquet for storage (D1); DuckDB for transforms (D8).

**Operations**
- Runs weekly on this laptop. Late arrivals don't matter (Q10, D5).
- Problems go to a log file. A month that isn't published yet produces a warning once a threshold passes, which is still to be set (Q11, F9).

## 8. Open questions

Answer inline, or write "defer" with a reason.

- **F9 (Q11, D5; carried over).** Files land on day 5 of the following month at the median, and by day 26 at the latest since 2022 (§2). A run early in the month therefore usually finds last month missing, and that's normal. If every such run warns, you'll learn to ignore the warning and then miss a real one.
  - (a) After which day of the following month should a missing month produce a warning, instead of a "not yet published" log line? Would your answer change if runs were monthly instead of weekly (D5)?
  - (b) When will you read the log?
- **F22 (F14, F21, D7).** F14 picked B, which casts to the declared schema *in bronze*. Your clarification says raw and bronze should be the same, and that Parquet and schema enforcement start at silver. Three designs fit parts of that:
  1. Bronze is Parquet cast to the declared schema (B as written).
  2. Bronze is the downloaded files, and silver is the first Parquet, cast to the declared schema (your clarification as written).
  3. Bronze is Parquet with every column as text (D7-A), and silver casts to the declared schema.

  On your medallion question, Databricks' page says "the bronze layer contains raw, unvalidated data" and "maintains the raw state of the data source in its original formats". It also recommends "storing most fields as string, VARIANT, or binary to protect against unexpected schema changes", and it lists schema enforcement under silver. Bronze "ingests raw data from cloud storage, Kafka, and Salesforce"; the page doesn't describe a separate raw zone before it. Pick a design and say why the other two lose. Appendix A compares 1 and 3.
- **F23 (F12, F13, D6).**
  - (a) Does "everything available" mean the 141 monthly trip files, or also the yearly `hubway_Trips_2011…2014` files and the station lists that share the prefix?
  - (b) Months before 202304 use the old schema (§2): no `ride_id`, `usertype` instead of `member_casual`, and station IDs like `386` instead of `"A32011"`. Does silver's declared schema cover those months, or does silver start at 202304? Fine to defer until you've looked at the data.
  - (c) F13 was deferred until a new schema arrives, but with F12 two schemas arrive on the first run. Under F22 design 1 or 3, F13 is due now. Under design 2, it moves to silver.
- **F24 (F17, Q4, D3).**
  - (a) You wrote that a score "should follow the actuals that the data was trained on." A forecast for October is scored against October's actuals, which weren't in its training data. Which actuals did you mean? If you meant October as first published, that reverses Q4, and D3 can't deliver it: overwrite keeps only the latest release, and the bucket doesn't keep old versions either (§2).
  - (b) On F17(b): your answer, a separate table, is right. Does D3's overwrite ever touch that table? With that in mind, reread your F8 answer: "The original forecast would be largely moot."
  - (c) Still open from F17: which fact in §2 justifies D3 better than "moot"?
- **F25 (F15, D1, D8).**
  - (a) F15 compares Parquet only with CSV. Now that DuckDB runs your transforms (D8), the closer rival is a DuckDB database file (D1-C). Why do Parquet files beat it? The comparison is in Appendix A.
  - (b) You cite "memory efficiency". Do you mean RAM or disk, and through what mechanism? (F10 mixed the two up.)
- **F26 (D8).** Polars' docs say "polars != pandas". Polars stores data in Arrow format rather than numpy, has no index and is strict about types. Its lazy engine pushes filters and column selection down into the file scan, as DuckDB does with Parquet. Sources: https://docs.pola.rs/user-guide/migration/pandas/ and https://docs.pola.rs/user-guide/lazy/optimizations/. Given that, where does the real difference between polars and DuckDB lie? Restate why DuckDB wins and why polars loses. Logged as a misconception in LEARNING_LOG.

## 9. Next steps

1. Answer F9 and F22–F26 inline.
2. Once D7 is settled (F22), draft the module split and pseudo-code together, one piece at a time.

## Appendix A. Option tables and pointers

### D1. Stored format

|  | A. CSV, schema applied on read | B. Parquet, cast once | C. DuckDB database file with constraints |
|--|--|--|--|
| Where the schema lives | In reader code; every script that reads the data must apply the same one | In each file's metadata | In the table definition |
| When a bad value surfaces | At read time, in whichever script hits it | During the run that does the cast (D7) | During the run (the insert is rejected) |
| Strictness available | Types only | Types, plus a choice of `CAST` (fail) or `TRY_CAST` (NULL) | Types, `NOT NULL`, `CHECK`, `PRIMARY KEY`, `ENUM` |
| Where the reject table lives | A separate file | A separate Parquet dataset | Another table in the same file |
| Read cost | Re-parses the text on every read | Columnar; reads only the columns a query needs | Columnar, native storage |
| Concurrency | Any number of readers | Any number of readers | One read-write process at a time; an open interactive session (a prototyping notebook, the DuckDB CLI) can block the scheduled run |
| Applying a revision | Replace the file | Rewrite that month's file (Parquet files are immutable) | `DELETE` + `INSERT` for the month in one transaction |
| Portability | Anything can read it | pandas, polars, Arrow, Spark, R `arrow` | Mostly DuckDB; a DuckDB-specific file |

Pointers:
- pandas `read_parquet` (see `dtype_backend`): https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html
- pyarrow ↔ pandas conversion and nullable types: https://arrow.apache.org/docs/python/pandas.html
- DuckDB Python result conversion (`.df()`, `.fetchnumpy()`, `.arrow()`): https://duckdb.org/docs/current/clients/python/conversion.html
- DuckDB `CAST` vs. `TRY_CAST`: https://duckdb.org/docs/current/sql/expressions/cast.html
- DuckDB `ENUM`: https://duckdb.org/docs/current/sql/data_types/enum.html; constraints: https://duckdb.org/docs/current/sql/constraints.html; concurrency: https://duckdb.org/docs/current/connect/concurrency.html
- pandera (dataframe schema validation, a candidate for silver): https://pandera.readthedocs.io/en/stable/

### D7. Bronze typing (for F22)

Design 2 has no bronze table, so it isn't in this comparison: silver reads the downloaded files directly.

|  | All strings in bronze (D7-A; F22 design 3) | Declared schema in bronze (D7-B; F22 design 1) |
|--|--|--|
| How | Read every column as text (DuckDB `all_varchar`) | A schema file committed to the repo; cast on ingest |
| Type problems in bronze | Never; only structural parse errors (wrong field count, broken quoting) | Failed rows go to the reject table (Q7) |
| What F2's check compares in bronze | Names only (every type is text) | Names and declared types |
| Upkeep | None in bronze; silver owns the casts | Edit the schema file when the source changes |
| Reversibility | Exact source text kept (leading zeros, timestamp precision) | A wrong cast loses the original text, though raw still has it |

Pointers: DuckDB `read_csv` options (`all_varchar`, `sample_size`, `store_rejects`): https://duckdb.org/docs/current/data/csv/overview.html; CSV auto-detection: https://duckdb.org/docs/current/data/csv/auto_detection.html; pyarrow `ConvertOptions(column_types=...)`: https://arrow.apache.org/docs/python/generated/pyarrow.csv.ConvertOptions.html. Search term: schema-on-read vs. schema-on-write.

### D8. Dataframe library / transform engine

|  | A. pandas | B. polars | C. DuckDB (SQL) for transforms |
|--|--|--|--|
| Familiarity | High, from your forecasting work | New API | SQL is familiar; DuckDB specifics are new |
| Performance at a few GB | Eager and in-memory; adequate but the slowest of the three | Multi-threaded, with lazy scans over Parquet (`scan_parquet`) | Multi-threaded, can spill to disk, reads Parquet directly |
| Typing | numpy dtypes by default; optional pyarrow backend with nullable types | Arrow types, strict casts | SQL types, `TRY_CAST` |
| Fit with modeling libraries | The common input for statsmodels formulas and many PyMC examples | Convert at the model boundary (`to_pandas()`, `to_numpy()`) | Convert at the model boundary (`.df()`, `.pl()`, `.fetchnumpy()`) |
| Fit with "scripts and systems" | Python modules | Python modules | Versioned `.sql` files plus a thin Python runner; two languages |

All three end at the same boundary: the model receives numpy arrays or tensors. In pandas (default backend) and polars (`to_numpy()`), an integer column with nulls becomes float with NaN at that point (F20).

Pointers: polars user guide: https://docs.pola.rs/; polars, coming from pandas: https://docs.pola.rs/user-guide/migration/pandas/; polars lazy optimizations: https://docs.pola.rs/user-guide/lazy/optimizations/; `Series.to_numpy` (null handling): https://docs.pola.rs/api/python/stable/reference/series/api/polars.Series.to_numpy.html; pandas pyarrow backend: https://pandas.pydata.org/docs/user_guide/pyarrow.html; pandas nullable integers: https://pandas.pydata.org/docs/user_guide/integer_na.html; DuckDB Python API: https://duckdb.org/docs/current/clients/python/overview.html

### D2, D3, D5

- D2: S3 `Object` (ETag rules): https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html; S3 object integrity: https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html; S3 versioning (`null` version IDs): https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html
- D3: Croushore & Stark (2001), "A real-time data set for macroeconomists", *Journal of Econometrics* 105(1), 111–130; Fowler, "Bitemporal History": https://martinfowler.com/articles/bitemporal-history.html
- D5: `TaskSettings.StartWhenAvailable`: https://learn.microsoft.com/en-us/windows/win32/taskschd/tasksettings-startwhenavailable. Search term: idempotent batch jobs.
