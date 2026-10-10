import pytest

from bluebikes.source import (
    BUCKET_URL,
    SourceError,
    SourceFile,
    download,
    extract_csv,
    latest_month,
    list_bucket,
    month_range,
    parse_listing,
    resolve_months,
)
from conftest import FakeResponse, listing_xml, trip_csv, zip_bytes

# Trip-file keys in the bucket for 2023-10..2026-09 as listed on 2026-10-07, plus neighbours and non-trip objects.
WINDOW_KEYS = [f"{y}{m:02d}-bluebikes-tripdata.zip" for y, m in
               [(2023, 10), (2023, 11), (2023, 12)]
               + [(y, m) for y in (2024, 2025) for m in range(1, 13)]
               + [(2026, m) for m in range(1, 10)]]
WINDOW_KEYS[WINDOW_KEYS.index("202511-bluebikes-tripdata.zip")] = "202511-bluebikes-tripdata.csv.zip"
WINDOW_KEYS[WINDOW_KEYS.index("202609-bluebikes-tripdata.zip")] = "202609-bluebikes-tripdata.csv"
OTHER_KEYS = [
    "202309-bluebikes-tripdata.zip", "201804-hubway-tripdata.zip", "hubway_Trips_2014_1.csv",
    "Hubway_Stations_2011_2016.csv", "previous_Hubway_Stations_as_of_July_2017.csv", "index.html",
]


def files_for(keys):
    return [SourceFile(k, "etag", "2026-01-01T00:00:00.000Z", 1) for k in keys]


def test_parse_listing_reads_fields_and_strips_etag_quotes():
    xml = listing_xml([SourceFile("202401-bluebikes-tripdata.zip", "abc123", "2024-02-05T15:30:13.000Z", 42)], token="tok")
    files, token = parse_listing(xml)
    assert files == [SourceFile("202401-bluebikes-tripdata.zip", "abc123", "2024-02-05T15:30:13.000Z", 42)]
    assert token == "tok"


def test_list_bucket_follows_continuation_tokens():
    pages = {
        None: listing_xml(files_for(["a"]), token="t1"),
        "t1": listing_xml(files_for(["b"]), token="t2"),
        "t2": listing_xml(files_for(["c"])),
    }

    class PagedSession:
        def get(self, url, params=None, timeout=None):
            assert url == BUCKET_URL and params["list-type"] == "2"
            return FakeResponse(pages[params.get("continuation-token")])

    assert [f.key for f in list_bucket(PagedSession())] == ["a", "b", "c"]


def test_month_range_crosses_years_inclusively():
    assert month_range("2023-11", "2024-02") == ["2023-11", "2023-12", "2024-01", "2024-02"]
    assert month_range("2024-05", "2024-05") == ["2024-05"]


@pytest.mark.parametrize("start,end", [("2024-03", "2024-02"), ("2024-13", "2024-12"), ("202401", "2024-02")])
def test_month_range_rejects_bad_input(start, end):
    with pytest.raises(ValueError):
        month_range(start, end)


def test_resolve_project_window_handles_naming_quirks():
    resolved = resolve_months(files_for(WINDOW_KEYS + OTHER_KEYS), month_range("2023-10", "2026-09"))
    assert len(resolved) == 36
    assert resolved["2023-10"].key == "202310-bluebikes-tripdata.zip"
    assert resolved["2025-11"].key == "202511-bluebikes-tripdata.csv.zip"
    assert resolved["2026-09"].key == "202609-bluebikes-tripdata.csv"


def test_latest_month_ignores_non_trip_objects():
    assert latest_month(files_for(WINDOW_KEYS + OTHER_KEYS)) == "2026-09"


def test_resolve_fails_on_missing_or_duplicate_month():
    keys = ["202401-bluebikes-tripdata.zip", "202401-bluebikes-tripdata.csv"]  # 2024-01 twice, 2024-02 missing
    with pytest.raises(SourceError) as err:
        resolve_months(files_for(keys), ["2024-01", "2024-02"])
    assert "2024-01: 2 candidate files" in str(err.value)
    assert "2024-02: 0 candidate files" in str(err.value)


def test_extract_ignores_macos_metadata(tmp_path):
    csv = trip_csv("2024-01")
    archive = tmp_path / "202401-bluebikes-tripdata.zip"
    archive.write_bytes(zip_bytes({
        "202401-bluebikes-tripdata.csv": csv,
        "__MACOSX/._202401-bluebikes-tripdata.csv": b"\x00\x05\x16\x07resource-fork",
    }))
    out = extract_csv(archive, tmp_path / "work")
    assert out.read_bytes() == csv
    assert out.parent == tmp_path / "work"


def test_extract_handles_csv_zip_name(tmp_path):
    archive = tmp_path / "202511-bluebikes-tripdata.csv.zip"
    archive.write_bytes(zip_bytes({"nested/dir/202511-bluebikes-tripdata.csv": trip_csv("2025-11")}))
    out = extract_csv(archive, tmp_path / "work")
    assert out == tmp_path / "work" / "202511-bluebikes-tripdata.csv"  # fixed name, not the archive's path


@pytest.mark.parametrize("members", [
    {"readme.txt": b"no csv here"},
    {"a.csv": b"x", "b.csv": b"y"},
])
def test_extract_rejects_zip_without_exactly_one_csv(tmp_path, members):
    archive = tmp_path / "202401-bluebikes-tripdata.zip"
    archive.write_bytes(zip_bytes(members))
    with pytest.raises(SourceError, match="expected 1 CSV"):
        extract_csv(archive, tmp_path / "work")


def test_extract_passes_raw_csv_through(tmp_path):
    raw = tmp_path / "202609-bluebikes-tripdata.csv"
    raw.write_bytes(trip_csv("2026-09"))
    assert extract_csv(raw, tmp_path / "work") == raw


def test_download_reuses_same_etag_and_refetches_on_change(tmp_path, bucket):
    key = "202401-bluebikes-tripdata.zip"
    bucket.put(key, b"version one")
    src = list_bucket(bucket)[0]
    path = download(bucket, src, tmp_path)
    assert download(bucket, src, tmp_path) == path
    assert bucket.downloads == [key]  # second call reused the file on disk

    bucket.put(key, b"version two!")
    revised = list_bucket(bucket)[0]
    assert download(bucket, revised, tmp_path).read_bytes() == b"version two!"
    assert bucket.downloads == [key, key]


def test_download_rejects_truncated_file(tmp_path, bucket):
    bucket.put("202401-bluebikes-tripdata.zip", b"short")
    src = list_bucket(bucket)[0]
    lying = SourceFile(src.key, src.etag, src.last_modified, size=src.size + 10)
    with pytest.raises(SourceError, match="downloaded 5 bytes"):
        download(bucket, lying, tmp_path)
    assert not (tmp_path / src.key).exists()
