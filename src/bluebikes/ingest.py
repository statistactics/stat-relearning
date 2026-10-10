"""Raw ingest: Bluebikes monthly trip files -> Parquet, one partition per source file.

Design decisions (user's, 2026-10-07):
- Keep the source schema: no renames, no derived columns, source row order kept.
- Partition by source-file month: data/parquet/trips/month=YYYY-MM/data.parquet.
- Types inferred per file by DuckDB, then a cross-month schema check before a partition goes live.
- Re-ingest a month only when its S3 key, ETag or LastModified changes (tracked in data/manifest.json).
- Timestamps stay as in the source: DuckDB reads them as TIMESTAMP without a time zone.

Run:  python -m bluebikes.ingest --start 2023-10 [--end YYYY-MM] [--data-dir data]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import requests

from bluebikes.source import (
    SourceError,
    SourceFile,
    download,
    extract_csv,
    latest_month,
    list_bucket,
    make_session,
    month_range,
    resolve_months,
)

log = logging.getLogger(__name__)

Schema = list[tuple[str, str]]  # (column name, DuckDB type), in column order


class SchemaMismatch(Exception):
    """A month's inferred schema differs from the partitions already in the dataset."""


@dataclass(frozen=True)
class Layout:
    """Where everything lives under the data directory."""

    root: Path

    @property
    def raw(self) -> Path:
        return self.root / "raw"

    @property
    def staging(self) -> Path:
        return self.root / "staging"

    @property
    def trips(self) -> Path:
        return self.root / "parquet" / "trips"

    @property
    def manifest(self) -> Path:
        return self.root / "manifest.json"

    def partition(self, month: str) -> Path:
        return self.trips / f"month={month}" / "data.parquet"


def _lit(path: Path) -> str:
    """Path as a SQL string literal (forward slashes, quotes escaped)."""
    return "'" + path.as_posix().replace("'", "''") + "'"


def convert(con: duckdb.DuckDBPyConnection, csv_path: Path, out_path: Path) -> int:
    """Write the CSV to Parquet with DuckDB-inferred types. Returns rows written.

    read_csv's sniffer infers delimiter, header and column types from a sample of rows
    (sample_size default 20,480). A later value that doesn't fit the sniffed type raises
    duckdb.ConversionException, which fails the month instead of silently widening the type.
    https://duckdb.org/docs/current/data/csv/auto_detection.html

    COPY ... (FORMAT parquet) defaults: Snappy compression, 122,880-row row groups.
    No ORDER BY, so rows keep source order.
    https://duckdb.org/docs/current/sql/statements/copy.html
    """
    (rows,) = con.execute(
        f"COPY (SELECT * FROM read_csv({_lit(csv_path)})) TO {_lit(out_path)} (FORMAT parquet)"
    ).fetchone()
    return rows


def parquet_schema(con: duckdb.DuckDBPyConnection, path: Path) -> Schema:
    """Column names and types of one Parquet file, read from its footer metadata.

    hive_partitioning is switched off because DuckDB auto-detects `key=value` folders and
    would add a virtual `month` column for files already in the dataset, but not for the
    staged file, making every comparison fail.
    https://duckdb.org/docs/current/data/partitioning/hive_partitioning.html
    """
    sql = f"DESCRIBE SELECT * FROM read_parquet({_lit(path)}, hive_partitioning = false)"
    return [(row[0], row[1]) for row in con.execute(sql).fetchall()]


def _describe_diff(existing: Schema, new: Schema) -> str:
    old, cur = dict(existing), dict(new)
    changes = [
        f"{col}: {old.get(col, 'missing')} -> {cur.get(col, 'missing')}"
        for col in dict.fromkeys([*old, *cur])
        if old.get(col) != cur.get(col)
    ]
    return "; ".join(changes) or "same columns and types, different order"


def check_schema(con: duckdb.DuckDBPyConnection, candidate: Path, layout: Layout, month: str) -> Schema:
    """Cross-month check: the candidate must match every existing partition exactly.

    The month being replaced is excluded. With no other partitions, the candidate is
    accepted, so the first month ingested becomes the reference.
    """
    new = parquet_schema(con, candidate)
    for other in sorted(layout.trips.glob("month=*/data.parquet")):
        if other.parent.name == f"month={month}":
            continue
        existing = parquet_schema(con, other)
        if existing != new:
            raise SchemaMismatch(f"{month} vs {other.parent.name}: {_describe_diff(existing, new)}")
    return new


def load_manifest(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def save_manifest(path: Path, manifest: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    tmp.replace(path)


def is_current(manifest: dict, month: str, src: SourceFile, layout: Layout) -> bool:
    """Skip only if this exact file version was ingested and its partition is still on disk."""
    entry = manifest.get(month)
    return (
        entry is not None
        and entry["key"] == src.key
        and entry["etag"] == src.etag
        and entry["last_modified"] == src.last_modified
        and layout.partition(month).exists()
    )


def ingest_month(
    con: duckdb.DuckDBPyConnection, session, layout: Layout, month: str, src: SourceFile, manifest: dict
) -> int:
    """Download -> extract -> convert in staging -> schema check -> promote -> record. Returns rows."""
    raw = download(session, src, layout.raw)
    work = layout.staging / month
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    try:
        csv_path = extract_csv(raw, work)
        candidate = work / "data.parquet"
        rows = convert(con, csv_path, candidate)
        schema = check_schema(con, candidate, layout, month)
        target = layout.partition(month)
        target.parent.mkdir(parents=True, exist_ok=True)
        # Staging and dataset share a volume, so this rename is atomic: readers see the
        # old partition or the new one, never a half-written file.
        os.replace(candidate, target)
        # A crash between the rename and this write leaves the month unrecorded, so the
        # next run re-ingests it. Same result, just repeated work.
        manifest[month] = {
            "key": src.key,
            "etag": src.etag,
            "last_modified": src.last_modified,
            "size": src.size,
            "rows": rows,
            "schema": schema,
            "ingested_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        save_manifest(layout.manifest, manifest)
        return rows
    finally:
        shutil.rmtree(work, ignore_errors=True)


def run(start: str, end: str | None, layout: Layout, session=None) -> dict[str, list[str]]:
    """Ingest every month from start to end (default: latest in the bucket).

    Resolution problems (a month with 0 or >1 files) stop the run before anything is
    downloaded. Per-month failures (schema mismatch, bad archive, conversion error,
    network error) are logged, leave that month's existing partition untouched, and the
    run moves on.
    """
    session = session or make_session()
    files = list_bucket(session)
    months = month_range(start, end or latest_month(files))
    sources = resolve_months(files, months)
    manifest = load_manifest(layout.manifest)
    result: dict[str, list[str]] = {"ingested": [], "skipped": [], "failed": []}
    with duckdb.connect() as con:
        for month in months:
            src = sources[month]
            if is_current(manifest, month, src, layout):
                log.info("%s skipped: %s unchanged", month, src.key)
                result["skipped"].append(month)
                continue
            try:
                rows = ingest_month(con, session, layout, month, src, manifest)
            except (SchemaMismatch, SourceError, duckdb.Error, requests.RequestException) as exc:
                log.error("%s failed: %s", month, exc)
                result["failed"].append(month)
                continue
            log.info("%s ingested: %d rows from %s", month, rows, src.key)
            result["ingested"].append(month)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest Bluebikes monthly trip files into partitioned Parquet.")
    parser.add_argument("--start", default="2023-10", help="first month, YYYY-MM (default: 2023-10)")
    parser.add_argument("--end", default=None, help="last month, YYYY-MM (default: latest in the bucket)")
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="data root (default: ./data)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = run(args.start, args.end, Layout(args.data_dir))
    log.info(
        "done: %d ingested, %d skipped, %d failed %s",
        len(result["ingested"]), len(result["skipped"]), len(result["failed"]), result["failed"] or "",
    )
    return 1 if result["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
