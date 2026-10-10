import json

import duckdb
import pytest

from bluebikes.ingest import Layout, convert, main, parquet_schema, run
from bluebikes.source import list_bucket, resolve_months
from conftest import trip_csv, zip_bytes

JAN, FEB = "202401-bluebikes-tripdata.zip", "202402-bluebikes-tripdata.zip"


def put_month(bucket, key, month, n=3, station_id="B32009", **kw):
    name = key.split(".", 1)[0] + ".csv"
    bucket.put(key, zip_bytes({name: trip_csv(month, n=n, station_id=station_id)}), **kw)


def query(sql):
    with duckdb.connect() as con:
        return con.execute(sql).fetchall()


def trips_glob(layout: Layout) -> str:
    return (layout.trips / "**" / "*.parquet").as_posix()


def test_convert_keeps_source_columns_with_inferred_types(tmp_path):
    csv = tmp_path / "t.csv"
    csv.write_bytes(trip_csv("2024-01", n=5))
    out = tmp_path / "t.parquet"
    with duckdb.connect() as con:
        assert convert(con, csv, out) == 5
        schema = dict(parquet_schema(con, out))
    assert list(schema) == [
        "ride_id", "rideable_type", "started_at", "ended_at",
        "start_station_name", "start_station_id", "end_station_name", "end_station_id",
        "start_lat", "start_lng", "end_lat", "end_lng", "member_casual",
    ]
    assert schema["started_at"] == "TIMESTAMP"  # no time zone, as in the source
    assert schema["start_station_id"] == "VARCHAR"
    assert schema["start_lat"] == "DOUBLE"


def test_run_writes_one_partition_per_month(bucket, layout):
    put_month(bucket, JAN, "2024-01", n=3)
    put_month(bucket, FEB, "2024-02", n=4)

    result = run("2024-01", "2024-02", layout, session=bucket)

    assert result == {"ingested": ["2024-01", "2024-02"], "skipped": [], "failed": []}
    assert query(f"SELECT count(*) FROM read_parquet('{layout.partition('2024-01').as_posix()}')") == [(3,)]
    assert query(f"SELECT count(*) FROM read_parquet('{layout.partition('2024-02').as_posix()}')") == [(4,)]
    # All partitions query as one table, with the folder name exposed as a `month` column.
    assert query(
        f"SELECT month, count(*) FROM read_parquet('{trips_glob(layout)}', hive_partitioning=true) GROUP BY month ORDER BY month"
    ) == [("2024-01", 3), ("2024-02", 4)]
    manifest = json.loads(layout.manifest.read_text())
    assert manifest["2024-02"]["key"] == FEB and manifest["2024-02"]["rows"] == 4
    assert not list(layout.staging.glob("*"))  # staging cleaned up


def test_end_defaults_to_latest_month_in_bucket(bucket, layout):
    put_month(bucket, JAN, "2024-01")
    put_month(bucket, FEB, "2024-02")
    assert run("2024-01", None, layout, session=bucket)["ingested"] == ["2024-01", "2024-02"]


def test_rerun_without_changes_downloads_nothing(bucket, layout):
    put_month(bucket, JAN, "2024-01")
    put_month(bucket, FEB, "2024-02")
    run("2024-01", "2024-02", layout, session=bucket)
    manifest_before = layout.manifest.read_text()
    bucket.downloads.clear()

    result = run("2024-01", "2024-02", layout, session=bucket)

    assert result == {"ingested": [], "skipped": ["2024-01", "2024-02"], "failed": []}
    assert bucket.downloads == []
    assert layout.manifest.read_text() == manifest_before


def test_revised_file_reingests_only_that_month(bucket, layout):
    put_month(bucket, JAN, "2024-01", n=3)
    put_month(bucket, FEB, "2024-02", n=3)
    run("2024-01", "2024-02", layout, session=bucket)
    bucket.downloads.clear()

    put_month(bucket, FEB, "2024-02", n=7, last_modified="2026-02-01T00:00:00.000Z")  # Bluebikes republishes Feb
    result = run("2024-01", "2024-02", layout, session=bucket)

    assert result == {"ingested": ["2024-02"], "skipped": ["2024-01"], "failed": []}
    assert bucket.downloads == [FEB]
    assert query(f"SELECT count(*) FROM read_parquet('{layout.partition('2024-02').as_posix()}')") == [(7,)]


def test_key_change_for_same_month_reingests(bucket, layout):
    put_month(bucket, JAN, "2024-01", n=3)
    run("2024-01", "2024-01", layout, session=bucket)

    bucket.delete(JAN)  # Bluebikes swaps the zip for a raw CSV
    bucket.put("202401-bluebikes-tripdata.csv", trip_csv("2024-01", n=5))
    assert run("2024-01", "2024-01", layout, session=bucket)["ingested"] == ["2024-01"]
    assert json.loads(layout.manifest.read_text())["2024-01"]["rows"] == 5


def test_deleted_partition_is_reingested(bucket, layout):
    put_month(bucket, JAN, "2024-01")
    run("2024-01", "2024-01", layout, session=bucket)
    layout.partition("2024-01").unlink()
    bucket.downloads.clear()

    assert run("2024-01", "2024-01", layout, session=bucket)["ingested"] == ["2024-01"]
    assert bucket.downloads == []  # raw file with the same ETag is reused, not re-downloaded
    assert layout.partition("2024-01").exists()


def test_schema_drift_rejects_new_month_and_leaves_dataset_untouched(bucket, layout):
    put_month(bucket, JAN, "2024-01")
    run("2024-01", "2024-01", layout, session=bucket)
    jan_bytes = layout.partition("2024-01").read_bytes()
    manifest_before = layout.manifest.read_text()

    put_month(bucket, FEB, "2024-02", station_id="32009")  # numeric-looking IDs: DuckDB infers BIGINT
    result = run("2024-01", "2024-02", layout, session=bucket)

    assert result["failed"] == ["2024-02"]
    assert not layout.partition("2024-02").exists()
    assert layout.partition("2024-01").read_bytes() == jan_bytes
    assert layout.manifest.read_text() == manifest_before


def test_schema_drift_in_revised_month_keeps_previous_partition(bucket, layout):
    put_month(bucket, JAN, "2024-01")
    put_month(bucket, FEB, "2024-02", n=3)
    run("2024-01", "2024-02", layout, session=bucket)
    feb_bytes = layout.partition("2024-02").read_bytes()

    put_month(bucket, FEB, "2024-02", n=9, station_id="32009", last_modified="2026-02-01T00:00:00.000Z")
    result = run("2024-01", "2024-02", layout, session=bucket)

    assert result["failed"] == ["2024-02"]
    assert layout.partition("2024-02").read_bytes() == feb_bytes
    assert json.loads(layout.manifest.read_text())["2024-02"]["rows"] == 3
    # Still recorded as the old version, so the next run tries the revision again.
    assert run("2024-01", "2024-02", layout, session=bucket)["failed"] == ["2024-02"]


def test_value_outside_sniffer_sample_fails_the_month(bucket, layout):
    # DuckDB infers types from the first 20,480 rows (sample_size default). Rows past that
    # which don't fit the inferred type raise a ConversionException instead of widening it.
    csv = trip_csv("2024-01", n=25_000, station_id="32009")
    lines = csv.decode().splitlines()
    lines[-1] = lines[-1].replace('"32009"', '"B32009"')
    bucket.put("202401-bluebikes-tripdata.csv", ("\n".join(lines) + "\n").encode())

    result = run("2024-01", "2024-01", layout, session=bucket)

    assert result["failed"] == ["2024-01"]
    assert not layout.partition("2024-01").exists()
    assert not layout.manifest.exists()


def test_main_exits_nonzero_when_a_month_fails(bucket, layout, monkeypatch):
    put_month(bucket, JAN, "2024-01")
    put_month(bucket, FEB, "2024-02", station_id="32009")
    monkeypatch.setattr("bluebikes.ingest.make_session", lambda: bucket)
    argv = ["--start", "2024-01", "--end", "2024-02", "--data-dir", str(layout.root)]
    assert main(argv) == 1
    assert main(["--start", "2024-01", "--end", "2024-01", "--data-dir", str(layout.root)]) == 0


@pytest.mark.network
def test_live_bucket_resolves_project_window():
    from bluebikes.source import make_session, month_range

    files = list_bucket(make_session())
    resolved = resolve_months(files, month_range("2023-10", "2026-09"))
    assert len(resolved) == 36
