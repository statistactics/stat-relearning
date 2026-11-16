# Monthly Bluebikes ingest: design (draft)

- **Status:** draft, updated 2026-11-11 with your answers to F1–F11 and your picks for D1, D2 and D5.
  - **Decided:** D1 (Parquet), D2 (A then B), D3 (overwrite), D4 (format-agnostic ingest), D5 (weekly).
  - **Open:** D6 (reopened by F1 and F3), D7 (where types are enforced), D8 (dataframe library).
- **Decision owner:** Caleb. Claude drafted the source facts, options and questions, records the answers, and adds a **Review** note to each one. Per CLAUDE.md, no option is recommended.
- **How to use this doc:** answer the reworded F9 and the new follow-ups F12–F21 in §7, then weigh D7 and D8 in §5.

## 1. Goal

Each month, fetch the newest Bluebikes trip file from the public `hubway-data` bucket (https://s3.amazonaws.com/hubway-data/), plus any earlier month that was re-published, and store the data so Bayesian and causal analyses can read it with known, enforced types. The newest month also provides the actuals that earlier forecasts are scored against.

### Principles

- **Scripts and systems, not notebooks.** Every pipeline step is a script, run by the scheduler or from the command line. Notebooks are only for occasional experimentation and prototyping. Nothing in the pipeline reads from or depends on a notebook.
- **Medallion layers.** Raw → bronze → silver → gold, as laid out in §4.

### Requirements

| ID | Requirement |
|----|-------------|
| R1 | Read individual monthly `.csv` files from the bucket. |
| R2 | Store them in a format suitable for inference and prediction with Bayesian and causal methods. This means the stored and read formats (Q3). |
| R3 | CSV is acceptable, but offer an alternative with stricter typecasting. The format is Parquet (D1); the layer where types are enforced is D7. |
| R4 | Run unattended on this laptop, weekly (D5). |
| R5 | Pull the latest month to compare against forecasts; scores use the latest release (Q4). |
| R6 | Pick up revisions to **any** month, and rewrite a month's partition when its source changed (Q5, Q6; originally only the previous month). |

### Still unknown

- The warning threshold for a month that hasn't been published (F9).
- The history window (F12).
- Where types are enforced (D7) and which dataframe library to use (D8).
- The forecasting and causal objectives (Q1, Q2). These only affect gold.

## 2. The source, as checked on 2026-11-02

Checked with anonymous `ListObjectsV2` and `ListObjectVersions` calls, a `HEAD` request, and by reading the headers of the 202303, 202304 and 202609 files.

| Fact | Detail | Why it matters |
|------|--------|----------------|
| Listing size | 150 objects on one page (`IsTruncated=false`; a page holds up to 1,000 keys). | Checking every month for changes costs one request. |
| Coverage | 141 monthly trip files, 201501–202609. 202610 was not published as of 2026-11-02. | The latest month at the time of the check was 202609. |
| Volume | 202304–202609 is 42 files and 751 MB as stored: 41 zips plus the 133 MB plain CSV for 202609. The 202606–202608 zips average 21.8 MB, against 132.8 MB for the 202609 CSV. | Sizes the disk use (F10) and the cost of option B in D2. |
| Publish lag | For data months from 2022 on, 56 of 57 files have a `LastModified` in the following month, between day 1 and day 26 (median: day 5). 7 of the 56 are after the 10th; the latest was 202312, on 2024-01-26. | Sets the warning threshold (F9). |
| Key names drift | `YYYYMM-hubway-tripdata.zip` → `YYYYMM-bluebikes-tripdata.zip` → `202511-bluebikes-tripdata.csv.zip` → `202609-bluebikes-tripdata.csv` (plain CSV). The zips also contain a `__MACOSX/._*.csv` member. Station lists and the yearly `hubway_Trips_2011…2014` files share the prefix. | Mapping a key to its month needs a rule, not one fixed pattern. Most months are zipped, not bare `.csv`. |
| Schema break | Up to 202303: `tripduration, starttime, stoptime, start station id, …, bikeid, usertype, postal code`. From 202304: `ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, …, member_casual`. | History starts at 202304 (F3, F12). |
| Type traps in the samples | Station IDs: `386` (integer-like, 202303) vs `"A32011"` (202304 on). `postal code` values such as `02139` have a leading zero. Timestamp precision: 4 decimals (202303), whole seconds (202304), milliseconds (202609). No timestamp carries a time-zone offset. | Inferred types depend on what the sniffer happens to see. DuckDB samples 20,480 rows by default, and an integer type would turn `02139` into `2139` (D7). |
| **Versioning is off** | `ListObjectVersions` returns exactly 150 entries, all with `VersionId=null`, including 202609, written 2026-10-05. | An overwrite destroys the previous version. Under D3, that's acceptable: you keep only the latest. |
| ETags aren't always content hashes | The bucket uses SSE-S3. Single-part uploads have MD5 ETags. Multipart uploads carry a `-N` suffix (202609 has `-8`), and those ETags are not MD5 digests. | Re-uploading identical bytes with a different part size changes the ETag (D2). |
| Revisions are rare and only partly visible | Among data months from 2022 on, only 202308 was last written more than a month later: at 2023-10-12 05:40:30, two seconds before 202309 was uploaded. Before 2021 there were bulk re-uploads (e.g. 201906–201908, all on 2020-11-17). | `LastModified` shows only the most recent write. Q5 checks every month, so it doesn't matter which months get revised. |

## 3. Assumptions

Each assumption is stated so it can be tested or corrected.

| ID | Assumption | Evidence so far | How to test | If wrong |
|----|------------|-----------------|-------------|----------|
| A1 | The `YYYYMM` prefix of a key is the month of the trips inside, and there is at most one trip file per month. | True for all 141 keys today. | Each run: count trip keys per `YYYYMM`; check the min and max start time inside the file. | Two keys for one month (e.g. `.zip` and `.csv`) leave it unclear which one counts. |
| A2 | A month's file holds the trips that *started* in that month. | Unverified. | Min and max of `started_at` per file. | Trips that cross a month boundary appear twice, or not at all. |
| A3 | `ride_id` is unique within and across files (202304 on). | Unverified. | Row count vs. distinct `ride_id` count. | No row key for de-duplication in silver, or for your F5 idea. |
| A4 | Timestamps are Boston wall-clock time (America/New_York). Kept that way (Q9); DST is handled in silver (F11). | The files don't say. I haven't found it documented. | Commute peaks should sit near 08:00 and 17:00; look at durations around the DST changes (e.g. 2025-11-02 01:00–02:00). | Hour-of-day features and durations shift. |
| A5 | A revision overwrites the same key instead of adding a new one. | 202308. | A1's per-month key count. | A revision looks like a duplicate month. |
| A6 | *Superseded by Q5: every month is checked for revisions.* | — | — | — |
| A7 | Volume stays laptop-sized. | 751 MB as stored for 202304–202609 (§2). No hard disk limit (F10). | Log bytes and rows per run. | Download and rewrite costs change the tradeoffs in D1 and D2. |
| A8 | It runs on this Windows 11 laptop, which can be asleep or off at the scheduled time. | Confirmed (Q10). | — | — |
| A9 | Nothing already in the repo constrains this design. | The README no longer describes the reverted ingest, and `pyproject.toml` was removed (2026-11-04). | — | — |
| A10 | The bucket stays public and anonymous, and your use is covered by Bluebikes' data license. | I haven't read the license terms. | Read the license, linked from https://bluebikes.com/system-data. | — |
| A11 | Bronze is partitioned by source month, so a revision rewrites exactly one partition. | My reading of "rewrite that partition" (Q6). Not contradicted so far. | — | Tell me if you meant something else. |
| A12 | History starts at a hardcoded 202304 and nothing walks backwards any more. A later schema change is accepted into bronze and reconciled in silver. | My reading of F1 and F3 together. | Your answer to F12. | D6 changes, and so does the bronze volume. |

## 4. Layers (medallion structure)

From Q2, Q7, Q8, Q12, F1, F10 and F11. Each layer is written by a script, never by a notebook.

| Layer | Holds | Rules | Status |
|-------|-------|-------|--------|
| Raw | The files as downloaded. | Latest version only, replaced when the source changes (Q12). Stays zipped to save disk (F10). | Decided. What happens to months published as plain CSV is open (F21). |
| Bronze | Source columns, mostly as published, in Parquet (D1). One partition per month (A11). | Accepts whatever schema a month arrives with (F1). No check stops a month from going live (Q8). | Typing and rejects depend on D7 (F14). Coexisting schemas: F13. |
| Silver | Restructured, validated trips. | Schema reconciliation (F1), validation, de-duplication, derived columns, empty vs. NULL (F4) and DST handling (F11). Type casting also lands here if D7 goes to option A. | Rules not yet defined. |
| Gold | Final model-ready datasets for training and evaluation: hierarchical forecasting inputs and causal panels. | Built by a later step, once the forecasting and causal work are scoped (Q1, Q2). | Deferred. |

Note for the bronze step: DuckDB's `read_csv` decompresses gzip and zstd but not zip. Reading raw therefore means taking the CSV member out of each zip first (Python's `zipfile`) and skipping the `__MACOSX` entry. This is incidental code; no decision is needed.

Pointers: the term "medallion architecture" comes from Databricks, but the pattern doesn't depend on their platform. https://www.databricks.com/glossary/medallion-architecture, https://docs.databricks.com/aws/en/lakehouse/medallion

## 5. Design decisions

### D1. Stored format (R2, R3): **Decided: B, Parquet**

**Your pick:** "Parquet seems suitable, I need experience with that file format and it appears storage efficient while being portable with common machine learning packages."

**Review:** Two of the three reasons hold. Wanting experience with Parquet is a fair reason in a learning project, and the storage saving is real. The third, portability with machine learning packages, contradicts your own F6 finding that those packages don't read files at all. F15 asks you to justify the pick again without it.

|  | A. CSV, schema applied on read | B. Parquet, cast once at ingest | C. DuckDB database file with constraints |
|--|--|--|--|
| Where the schema lives | In reader code; every script that reads the data must apply the same one | In each file's metadata | In the table definition |
| When a bad value surfaces | At read time, in whichever script hits it | During the run that does the cast (bronze or silver, D7) | During the monthly run (the insert is rejected) |
| Strictness available | Types only | Types, plus a choice of `CAST` (fail) or `TRY_CAST` (NULL) | Types, `NOT NULL`, `CHECK`, `PRIMARY KEY`, `ENUM` |
| Where the reject table lives | A separate file | A separate Parquet dataset | Another table in the same file |
| Read cost | Re-parses the text on every read | Columnar; reads only the columns a query needs | Columnar, native storage |
| Concurrency | Any number of readers | Any number of readers | One read-write process at a time; an open interactive session (a prototyping notebook, the DuckDB CLI) can block the scheduled run |
| Applying a revision | Replace the file | Rewrite that month's file (Parquet files are immutable) | `DELETE` + `INSERT` for the month in one transaction |
| Portability | Anything can read it | pandas, polars, Arrow, Spark, R `arrow` | Mostly DuckDB; a DuckDB-specific file |

**Compatibility with modeling libraries (Q3, F6).** Modeling libraries take in-memory arrays or DataFrames, so the format only has to suit the reader library: pyarrow, pandas, polars or DuckDB. Where types can change is the conversion from the reader to numpy (F20, D8).

Pointers:
- pandas `read_parquet` (see `dtype_backend`): https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html
- pyarrow ↔ pandas conversion and nullable types: https://arrow.apache.org/docs/python/pandas.html
- DuckDB Python result conversion (`.df()`, `.fetchnumpy()`, `.arrow()`): https://duckdb.org/docs/current/clients/python/conversion.html
- DuckDB `CAST` vs. `TRY_CAST`: https://duckdb.org/docs/current/sql/expressions/cast.html
- DuckDB `ENUM`: https://duckdb.org/docs/current/sql/data_types/enum.html; constraints: https://duckdb.org/docs/current/sql/constraints.html; concurrency: https://duckdb.org/docs/current/connect/concurrency.html
- pandera (dataframe schema validation, a candidate for silver): https://pandera.readthedocs.io/en/stable/

### D2. How a change is detected (R5, R6): **Decided: A then B**

**Your pick:** "A then B to avoid unnecessary rewrites."

- **Scope:** every month, on every run (Q5).
- **Granularity:** only *whether* a month changed (Q6).
- **Method:** compare S3 metadata (ETag, `LastModified`, size) against a local manifest. For a month that differs, download it and compare a content hash with the stored one. Rewrite the partition only if the hash differs.

**Review:** This still downloads every flagged month; what it saves is the rewrite. F16 asks whether that saving is worth the extra state, and what exactly gets hashed.

Pointers: S3 `Object` (ETag rules): https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html; S3 object integrity: https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html; S3 versioning (`null` version IDs): https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html.

### D3. What happens to old data when a month is revised (R5, R6): **Decided: A, overwrite**

From Q4 (score against the latest release) and Q12 (raw downloads stay until overwritten). Each month has one version in every layer.

Consequences you're accepting:
- A forecast score reported today can change after a revision, and the old score can't be reproduced.
- A backtest re-run later trains on data that the original forecast never saw.

**Your justification (F8):** "The re-score will be using the new data anyway. the original forecast would be largely moot."

**Review:** Half of this holds. Scoring a fixed forecast against the latest actuals is a defensible convention. But "the original forecast would be largely moot" doesn't follow: D3 covers the *source data*, not your forecasts, which are your own output. F17 splits the two questions apart.

Pointers: Croushore & Stark (2001), "A real-time data set for macroeconomists", *Journal of Econometrics* 105(1), 111–130; Fowler, "Bitemporal History": https://martinfowler.com/articles/bitemporal-history.html

### D4. What grain the pipeline delivers (R2): **Decided: format-agnostic ingest**

From Q2, the ingest delivers trip-level data (bronze, then silver). Panels belong in gold and are built by a later script once the causal work is scoped.

These questions move to the gold design:
- What are the unit and the period?
- A trip log records only trips that happened. How does a station-day with no trips show up in a panel, and how do you tell "no trips" apart from "the station didn't exist yet"? (See also F20.)

Search term: balanced vs. unbalanced panel.

### D5. Schedule and run behavior (R4): **Decided: B, weekly**

**Your pick:** "Weekly runs." Justification pending (F18).

- It runs on this laptop, and late arrivals don't matter (Q10).
- Every run checks every month (Q5), so any run catches up on what an earlier one missed.
- Task Scheduler can run a missed task once the laptop wakes (`StartWhenAvailable`).
- It logs to a file. When a missing month becomes a warning is open (F9).

Pointers: `TaskSettings.StartWhenAvailable`: https://learn.microsoft.com/en-us/windows/win32/taskschd/tasksettings-startwhenavailable. Search term: idempotent batch jobs.

### D6. History window (R2, schema break): **Reopened**

Q13 said to derive the schema from the latest month, walk backwards, and stop where the schema changes. F1 (accept any schema in bronze) and F3 (hardcode the boundary) replace that rule. My reading is A12: start at a hardcoded 202304, with no backward walk. F12 asks you to confirm it.

### D7. Where types are enforced: **Open**

This comes from your F1 proposal to "cast everything as string during the initial ingestion". It is option A below.

|  | A. All strings in bronze (your proposal) | B. Declared schema in bronze | C. Inferred schema in bronze |
|--|--|--|--|
| How | Read every column as text (DuckDB `all_varchar`) | A schema file committed to the repo; cast on ingest | Infer types per month from the whole file (`sample_size=-1`), not a sample |
| Type problems stop or reject rows in bronze? | Never; only structural parse errors (wrong field count, broken quoting) | Yes; failed rows go to the reject table (Q7) | Rarely; the inferred types fit whatever the file contains |
| Where R3's strictness lives | Silver | Bronze | Nowhere fixed; types follow the data |
| What F2's schema check compares | Names only (every type is a string) | Names and declared types | Names and inferred types, which can differ between months with identical headers (e.g. a column that's empty for a whole month) |
| Upkeep | None in bronze; silver owns the casts | Edit the schema file when the source changes | None |
| Reversibility | Highest: the exact source text is kept (leading zeros, timestamp precision) | A wrong cast loses the original text, though raw still has it | Depends on what was inferred |

Two parts of F1 need correcting:
- **"Sampling is the most efficient way."** At 133 MB or less per month, reading a whole file is cheap enough that efficiency shouldn't decide this. (I haven't timed it.)
- **"Safer."** Option A is safer for the ingest run, but the type risk doesn't go away; it moves to silver.

Questions: F14.

Pointers: DuckDB `read_csv` options (`all_varchar`, `sample_size`, `store_rejects`): https://duckdb.org/docs/current/data/csv/overview.html; CSV auto-detection: https://duckdb.org/docs/current/data/csv/auto_detection.html; pyarrow `ConvertOptions(column_types=...)`: https://arrow.apache.org/docs/python/generated/pyarrow.csv.ConvertOptions.html. Search term: schema-on-read vs. schema-on-write.

### D8. Dataframe library for the bronze write, silver, gold and model loading: **Open**

This comes from F7, where you asked for alternatives to pandas. Using more than one is common, e.g. SQL for the transforms and a dataframe library to load model inputs.

|  | A. pandas | B. polars | C. DuckDB (SQL) for transforms |
|--|--|--|--|
| Familiarity | High, from your forecasting work | New API | SQL is familiar; DuckDB specifics are new |
| Performance at a few GB | Eager and in-memory; adequate but the slowest of the three | Multi-threaded, with lazy scans over Parquet (`scan_parquet`) | Multi-threaded, can spill to disk, reads Parquet directly |
| Typing | numpy dtypes by default; optional pyarrow backend with nullable types | Arrow types, strict casts | SQL types, `TRY_CAST` |
| Fit with modeling libraries | The common input for statsmodels formulas and many PyMC examples | Convert at the model boundary (`to_pandas()`, `to_numpy()`) | Convert at the model boundary (`.df()`, `.pl()`, `.fetchnumpy()`) |
| Fit with "scripts and systems" | Python modules | Python modules | Versioned `.sql` files plus a thin Python runner; two languages |

All three end at the same boundary: the model receives numpy arrays or tensors. In pandas (default backend) and polars (`to_numpy()`), an integer column with nulls becomes float with NaN at that point (F20).

Pointers: polars user guide: https://docs.pola.rs/; `Series.to_numpy` (null handling): https://docs.pola.rs/api/python/stable/reference/series/api/polars.Series.to_numpy.html; pandas pyarrow backend: https://pandas.pydata.org/docs/user_guide/pyarrow.html; pandas nullable integers: https://pandas.pydata.org/docs/user_guide/integer_na.html; DuckDB Python API: https://duckdb.org/docs/current/clients/python/overview.html

### Out of scope for this draft

The module and file split, function signatures and tests. We draft those together once D7 and D8 are settled.

## 6. Source checks to repeat each run

Per Q8, these only log; none of them stops a month from going live. Mark the ones you want and drop the rest.

- [ ] Count trip keys per `YYYYMM`; flag more than one (A1, A5).
- [ ] Log when a month's column names differ from the previous month's (F1, F2; under D7-B or C, also its types).
- [ ] Min and max of the start time per file (A2).
- [ ] Row count vs. distinct `ride_id` (A3).
- [ ] Rejected-row count per month (Q7, D7).

## 7. Clarifying questions

### Round 1 (answered 2026-11-04)

**Purpose and consumers**
- **Q1 (D4, D3).** What do you forecast: which target, at what unit and grain, and over what horizon?
  **Answer:** Not confirmed yet. Likely a hierarchical forecasting exercise covering most levels, with flexible horizons, both short and long range.
- **Q2 (D4, D6).** For the causal work, what is the treatment, the unit and the period? If that isn't known yet, should the ingest commit to any panel at all?
  **Answer:** Not determined. The ingest should produce a format-agnostic dataset; panel conversion happens in a later step, once the causal work is scoped.
- **Q3 (D1, D4).** By "a format suitable for inference and prediction", do you mean the stored file format, the shape of the data, or both?
  **Answer:** The stored and read formats. The concern is compatibility with statistical packages: sklearn, statsmodels, TensorFlow and PyTorch are sure to work with numpy structures and pandas DataFrames, but it's unclear how they interface with other formats.

**Revisions and scoring**
- **Q4 (D3).** If a month is revised after you've scored a forecast against it, should that score change?
  **Answer:** Report against the latest release.
- **Q5 (D2).** Why check only M-1 for revisions?
  **Answer:** No reason. Check all months.
- **Q6 (D2).** Do you need to know *what* changed in a revision, or only *that* something changed?
  **Answer:** Only that something changed; then rewrite that partition.

**Types and quality**
- **Q7 (D1).** When a value can't be cast to its declared type, what should happen?
  **Answer:** Move the row to a reject table for further remediation.
- **Q8 (D1).** Which checks should stop a month from going live, and which should only warn?
  **Answer:** Nothing stops a month from going live at initial ingestion. Follow a medallion structure: bronze is ingested mostly as is (barring casting issues), silver does the restructuring and validation, and gold is the final form ready for model training and evaluation.
- **Q9 (D1, D4).** Local wall-clock time or UTC? Who handles the repeated hour at the November DST change?
  **Answer:** Keep local wall-clock time for simplicity. No answer yet for the repeated hour.

**Operations**
- **Q10 (D5).** Where will it run, and how late can the actuals arrive?
  **Answer:** On this laptop. Late arrivals don't matter.
- **Q11 (D5).** How should you hear about a failure? Is an unpublished month a success or a warning?
  **Answer:** A log file for now. If the new month isn't published yet, emit a warning.
- **Q12 (D3).** What's the disk budget, and how long do raw downloads stay?
  **Answer:** Not sure how to answer the disk budget. Raw downloads stay until overwritten.
- **Q13 (D6).** How far back should the history go?
  **Answer:** As far back as the schema allows. Derive the schema from the latest month, work backwards, and fail at the month where the schema changes.

### Round 2 (answered 2026-11-11)

**Schema and backfill (Q13, Q8)**
- **F1.** Is the schema derived from the latest month once and committed to the repo, or derived again on every run? Walk your rule through a hypothetical: 202611 arrives with one extra column. What does the backward walk keep, and how does that fit with Q8's "nothing should stop a month from going live"? If the schema is inferred, what makes the sampled types hold for the rest of the file?
  **Answer:** How about gather the schemas non-discriminately and just download them as they are, then reconcile in the silver layer. I have no answer for the inferred types, sampling is just the most efficient way of doing it for now. Maybe it's safer to cast everything as string during the initial ingestion?
  **Review:** Accepting any schema into bronze fixes the conflict with Q8: a new column in 202611 no longer cuts off the history. Casting everything to strings is a separate and bigger change, because it moves type enforcement from bronze to silver. It's now D7, option A, where the claims about efficiency and safety are also addressed. Follow-ups: F12–F14.
- **F2.** What counts as "the schema changed": column names, names and order, or names and types?
  **Answer:** Schema changes can be defined as column names and types for now. Order shouldn't matter much.
  **Review:** Ignoring order fits with reading columns by name. Under D7-A every bronze column is a string, so in bronze only the names can change. Since bronze now accepts any schema (F1), this check only logs (§6); it doesn't stop anything.
- **F3.** When the walk reaches 202303, is that a failed run or the expected end of the backfill? Is the boundary month recorded, or found again on every run?
  **Answer:** It should be the expected end of the backfill and can be hardcoded.
  **Review:** Fine. Taken with F1, I read this as A12: a hardcoded start month and no backward walk. Confirm in F12.

**Rejects (Q7)**
- **F4.** Is an empty field a reject or a legitimate NULL?
  **Answer:** Too early to answer definitively. Defer this until after ingestion.
  **Review:** Deferred to silver (§4).
- **F5.** When a revised month's partition is rewritten, what happens to that month's rows in the reject table?
  **Answer:** Defer this for when this case actually occurs. Initial idea is to search for the affected ride_ids and remove them from the reject table if they do not fail the ingestion.
  **Review:** Deferred. When it comes up, test your idea against two cases: a rejected row that isn't in the revised file at all, and a row rejected *because* it is malformed, whose `ride_id` may be unreadable.

**Formats and libraries (Q3)**
- **F6.** Which of sklearn, statsmodels, TensorFlow and PyTorch open files themselves, and what sits between a file on disk and a `fit()` call?
  **Answer:** I didn't do a thorough check, but it seems like none of these packages open files themselves. They read from an object in memory.
  **Review:** Right for sklearn, statsmodels, PyTorch and PyMC, with two exceptions:
  - TensorFlow's `tf.data` can read CSV files itself (`tf.data.experimental.make_csv_dataset`).
  - CmdStanPy's `sample(data=...)` accepts a path to a JSON or Rdump file.

  Neither changes the conclusion: the file format has to suit the *reader*, not the model. That conclusion undercuts part of your D1 justification (F15). I haven't checked HierarchicalForecast. This is logged as a misconception in LEARNING_LOG; your own summary is pending (F19).
- **F7.** Suppose bronze declares an integer column that has some NULLs. What dtype does it have once loaded into pandas?
  **Answer:** I haven't decided if I'm using pandas yet. I may want to experiment with more performant alternatives like polars, open to alternatives if you suggest any.
  **Review:** Switching libraries doesn't remove the question. The polars docs say an integer Series with nulls becomes float with NaN in `to_numpy()`, as it does in pandas by default. Also, today's bronze schema has no integer columns; counts first appear in gold. The question is reworded as F20, and the library options are now D8.

**Revisions and scoring (Q4)**
- **F8.** D3 means a reported score can change after a revision, and a backtest re-run later trains on data the original forecast never saw. Why is that acceptable here?
  **Answer:** The re-score will be using the new data anyway. The original forecast would be largely moot.
  **Review:** See D3. Follow-up: F17.

**Operations (Q11, Q12)**
- **F9 (reworded 2026-11-11; your earlier answer asked for clarification).** Your Q11 answer says a run should warn when the new month isn't published yet. With weekly runs, the first run each month usually happens before last month's file appears, because files land on day 5 at the median (§2). For example, a run on Monday 2 November looks for October and doesn't find it. That's normal: October usually appears around 5 November. If every such run warns, you'll get a warning almost every month that means nothing, learn to ignore it, and then miss a real one.
  - (a) After which day of the following month should a still-missing month count as late and produce a warning? Before that day, the run would log "not yet published" without a warning. For reference, the latest arrival since 2022 was day 26.
  - (b) Warnings only go to a log file. When will you look at it?
- **F10.** Will raw stay zipped or be extracted? Is there a disk limit you care about?
  **Answer:** Raw should stay zip to consume minimal memory. No hard limit.
  **Review:** Zipping saves *disk*, not memory: the data is decompressed into RAM whenever it's read. "No hard limit" closes the budget question. Follow-up: F21.

**DST (Q9)**
- **F11.** Can the repeated hour wait until silver, as long as bronze keeps the timestamps exactly as published?
  **Answer:** It can wait until silver.
  **Review:** Agreed. It's listed under silver in §4.

### Round 3 (open)

**History and schema (D6, F1, F3)**
- **F12.** I've read F1 and F3 together as A12: history starts at a hardcoded 202304, and any later schema change goes into bronze to be reconciled in silver. The other reading is that bronze takes every month back to 201501, in every schema. Which did you mean? If it's the first: now that bronze no longer depends on a single schema, why start at 202304? **Answer**: Valid point. Let's just ingest everything available into the bronze layer, then we'll see how it goes once processed into silver.
- **F13.** When a month with a new schema arrives, does it go into the same bronze dataset as earlier months, or into a separate dataset per schema version? In a shared dataset, a reader that matches columns by name (DuckDB's `union_by_name`) sees NULLs for the columns a month lacks. Fine to defer until it happens. **Answer**: Defer until this happens. Too early to say.

**Bronze typing (D7)**
- **F14.** Weigh D7's options and pick one. If you pick A, two things follow:
  - Bronze then holds the same text as raw, only in Parquet. What does bronze give you that raw doesn't?
  - Q7's reject table was for rows that fail a cast. Under A, casts happen in silver. Does the reject table move to silver, or does each layer get its own?

**Answer**: I will choose B. Clarifying the medallion architecture - shouldn't Bronze and Raw be the same thing? Schema enforcement and saving down to Parquet should only enter at the silver layer.


**Justifications**
- **F15 (D1).** Restate your D1 justification without "portable with common machine learning packages". Does Parquet still win? Against which option, and why does that option lose? **Answer**: Against CSV, Parquet would be preferred because of type casting and memory efficiency. I know it doesn't matter for this scale but for the purposes of learning, I find it valuable to use a format I'm not familiar with.
- **F16 (D2).** A then B still downloads every flagged month; what it saves is the rewrite. What does a needless rewrite of one month actually cost you, and is that worth storing a hash for every month? Also, will you hash the zip as downloaded or the CSV inside it? Re-zipping an unchanged CSV can produce different zip bytes. **Answer**: Likely not. Let's get rid of the hashing to keep things simpler.
- **F17 (D3).** F8 asked two questions and got one answer. Take them separately:
  - (a) Should a fixed forecast's score follow the latest actuals? **Answer**: Probably not. It should follow the actuals that the data was trained on.
  - (b) When you want to know how well your forecasting did *at the time*, where do the original forecasts and the dates they were made live? **Answer**: In a separate table on the laptop to evaluate against upcoming actuals? I'm not sure what the question is getting at.

  Separately, §2 holds evidence that justifies D3 better than "moot". What is it?
- **F18 (D5).** In one line: why weekly rather than one run a month on a fixed day? **Answer**: More opportunities for catch-up? It's arbitrary.

**Libraries (F6, F7, D8)**
- **F19.** Write the F6 insight in your own words for LEARNING_LOG: where does the file format stop mattering, and what takes over? **Answer**: The file format stops mattering once the data is read into memory. thereafter, the libraries interact with the data structures in memory rather than the file format. (e.g. numpy arrays)
- **F20 (reworded F7).** Whatever library loads the data, the model ends up with a numpy array or a tensor. Can a numpy integer array represent a missing value? Suppose a gold dataset has an integer column with gaps, such as trip counts for a station that didn't exist yet. At which step, and by whose rule, does each gap become a 0, a NaN or a dropped row? **Answer**: Yet to be decided, will be settled in the gold layer.
- **D8.** Weigh the options in §5 and pick, with a one-line justification. **Answer**: DuckDB to expand my exposure to dataframe packages. Polars seems like a natural extension/improvement on pandas whereas DuckDB seems like a different approach altogether.

**Raw (F10)**
- **F21.** 202609 was published as a plain 133 MB CSV. Does "raw stays zipped" mean you zip such files yourself, so raw is no longer exactly as downloaded? Or does raw keep whatever was published? **Answer**: Raw/Bronze should keep the file exactly as is.

## 8. Next steps

1. Answer F9 (reworded) and F12–F21 inline, or write "defer" with a reason.
2. Pick options for D7 and D8 with a one-line justification each.
3. Draft the module split and pseudo-code together, one piece at a time.
