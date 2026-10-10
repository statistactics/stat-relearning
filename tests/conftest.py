"""Test helpers: an in-memory fake of the S3 bucket and CSVs shaped like the real trip files."""

from __future__ import annotations

import hashlib
import io
import zipfile
from xml.sax.saxutils import escape

import pytest

from bluebikes.ingest import Layout
from bluebikes.source import BUCKET_URL, SourceFile

HEADER = [
    "ride_id", "rideable_type", "started_at", "ended_at",
    "start_station_name", "start_station_id", "end_station_name", "end_station_id",
    "start_lat", "start_lng", "end_lat", "end_lng", "member_casual",
]


def trip_csv(month: str, n: int = 3, station_id: str = "B32009") -> bytes:
    """A trip CSV in the source's style: strings quoted, numbers bare, one header row."""
    lines = [",".join(f'"{h}"' for h in HEADER)]
    for i in range(n):
        minute = f"{i % 60:02d}"
        lines.append(
            f'"R{i:015X}","classic_bike","{month}-15 08:{minute}:00.000","{month}-15 08:{minute}:30.000",'
            f'"Fenway at Forsyth Way","{station_id}","Fenway at Forsyth Way","{station_id}",'
            f'42.341,-71.092,42.341,-71.092,"member"'
        )
    return ("\n".join(lines) + "\n").encode()


def zip_bytes(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return buf.getvalue()


def listing_xml(files: list[SourceFile], token: str | None = None) -> bytes:
    """One ListObjectsV2 page, shaped like the real S3 response."""
    contents = "".join(
        f"<Contents><Key>{escape(f.key)}</Key><LastModified>{f.last_modified}</LastModified>"
        f"<ETag>&quot;{f.etag}&quot;</ETag><Size>{f.size}</Size><StorageClass>STANDARD</StorageClass></Contents>"
        for f in files
    )
    paging = f"<NextContinuationToken>{token}</NextContinuationToken>" if token else ""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        f"<Name>hubway-data</Name>{paging}<IsTruncated>{'true' if token else 'false'}</IsTruncated>"
        f"{contents}</ListBucketResult>"
    ).encode()


class FakeResponse:
    def __init__(self, content: bytes, status: int = 200):
        self.content = content
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size: int = 1):
        for i in range(0, len(self.content), chunk_size):
            yield self.content[i : i + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeBucket:
    """Stands in for requests.Session: serves the listing and object downloads from memory."""

    def __init__(self):
        self.objects: dict[str, tuple[bytes, str, str]] = {}  # key -> (data, etag, last_modified)
        self.downloads: list[str] = []

    def put(self, key: str, data: bytes, last_modified: str = "2026-01-05T00:00:00.000Z") -> None:
        self.objects[key] = (data, hashlib.md5(data).hexdigest(), last_modified)

    def delete(self, key: str) -> None:
        del self.objects[key]

    def get(self, url, params=None, stream=False, timeout=None):
        if url == BUCKET_URL:
            files = [SourceFile(k, etag, lm, len(d)) for k, (d, etag, lm) in sorted(self.objects.items())]
            return FakeResponse(listing_xml(files))
        key = url.removeprefix(BUCKET_URL)
        self.downloads.append(key)
        return FakeResponse(self.objects[key][0])


@pytest.fixture
def bucket() -> FakeBucket:
    return FakeBucket()


@pytest.fixture
def layout(tmp_path) -> Layout:
    return Layout(tmp_path / "data")
