"""Find, download and unpack Bluebikes monthly trip files from the public S3 bucket."""

from __future__ import annotations

import re
import shutil
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BUCKET_URL = "https://s3.amazonaws.com/hubway-data/"
TIMEOUT = 60  # seconds per HTTP request

_S3 = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

# Monthly trip files. Naming has drifted, so all three endings are accepted:
#   202310-bluebikes-tripdata.zip, 202511-bluebikes-tripdata.csv.zip, 202609-bluebikes-tripdata.csv
# Pre-2018-05 files use "hubway" instead of "bluebikes".
TRIP_KEY = re.compile(r"^(?P<ym>\d{6})-(?:hubway|bluebikes)-tripdata\.(?:zip|csv\.zip|csv)$")
_MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


class SourceError(Exception):
    """The bucket or a downloaded file doesn't match what the ingest design assumes."""


@dataclass(frozen=True)
class SourceFile:
    key: str
    etag: str
    last_modified: str
    size: int

    @property
    def url(self) -> str:
        return BUCKET_URL + self.key


def make_session() -> requests.Session:
    """HTTP session that retries transient server errors with backoff."""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=(500, 502, 503, 504))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def parse_listing(xml_bytes: bytes) -> tuple[list[SourceFile], str | None]:
    """Parse one ListObjectsV2 page into files and the next continuation token (None on the last page)."""
    root = ET.fromstring(xml_bytes)
    files = [
        SourceFile(
            key=c.findtext("s3:Key", namespaces=_S3),
            etag=c.findtext("s3:ETag", namespaces=_S3).strip('"'),
            last_modified=c.findtext("s3:LastModified", namespaces=_S3),
            size=int(c.findtext("s3:Size", namespaces=_S3)),
        )
        for c in root.findall("s3:Contents", _S3)
    ]
    truncated = root.findtext("s3:IsTruncated", namespaces=_S3) == "true"
    token = root.findtext("s3:NextContinuationToken", namespaces=_S3) if truncated else None
    return files, token


def list_bucket(session) -> list[SourceFile]:
    """Every object in the bucket, following continuation tokens across pages."""
    files: list[SourceFile] = []
    token = None
    while True:
        params = {"list-type": "2"}
        if token:
            params["continuation-token"] = token
        resp = session.get(BUCKET_URL, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        page, token = parse_listing(resp.content)
        files.extend(page)
        if token is None:
            return files


def month_range(start: str, end: str) -> list[str]:
    """Inclusive list of 'YYYY-MM' months from start to end."""
    for m in (start, end):
        if not _MONTH.match(m):
            raise ValueError(f"month must look like YYYY-MM, got {m!r}")
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    year, month = map(int, start.split("-"))
    months = []
    while (current := f"{year:04d}-{month:02d}") <= end:
        months.append(current)
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months


def _trip_month(key: str) -> str | None:
    m = TRIP_KEY.match(key)
    return f"{m['ym'][:4]}-{m['ym'][4:]}" if m else None


def latest_month(files: list[SourceFile]) -> str:
    """Most recent month that has a trip file in the bucket."""
    months = [m for f in files if (m := _trip_month(f.key))]
    if not months:
        raise SourceError("no trip files found in the bucket")
    return max(months)


def resolve_months(files: list[SourceFile], months: list[str]) -> dict[str, SourceFile]:
    """Map each month to exactly one trip file. Any month with 0 or >1 candidates fails the whole run."""
    candidates: dict[str, list[SourceFile]] = {}
    for f in files:
        if month := _trip_month(f.key):
            candidates.setdefault(month, []).append(f)
    problems = [
        f"{month}: {len(found)} candidate files {[f.key for f in found]}"
        for month in months
        if len(found := candidates.get(month, [])) != 1
    ]
    if problems:
        raise SourceError("cannot resolve months:\n  " + "\n  ".join(problems))
    return {month: candidates[month][0] for month in months}


def download(session, src: SourceFile, raw_dir: Path) -> Path:
    """Fetch src into raw_dir, reusing an earlier download of the same ETag.

    A `<key>.etag` sidecar records which version is on disk; the file is written to
    `<key>.part` and renamed only after its size matches the listing.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    dest = raw_dir / src.key  # key matched TRIP_KEY, so it has no path separators
    etag_file = dest.with_name(dest.name + ".etag")
    if dest.exists() and etag_file.exists() and etag_file.read_text() == src.etag:
        return dest
    part = dest.with_name(dest.name + ".part")
    with session.get(src.url, stream=True, timeout=TIMEOUT) as resp:
        resp.raise_for_status()
        with open(part, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    if (got := part.stat().st_size) != src.size:
        part.unlink()
        raise SourceError(f"{src.key}: downloaded {got} bytes, listing says {src.size}")
    part.replace(dest)
    etag_file.write_text(src.etag)
    return dest


def extract_csv(path: Path, work_dir: Path) -> Path:
    """Return a CSV for a downloaded trip file. A zip must contain exactly one CSV.

    macOS metadata entries (`__MACOSX/...`) are ignored. The CSV is written under a fixed
    name in work_dir, never at the path stored in the archive.
    """
    if path.name.endswith(".csv"):
        return path
    with zipfile.ZipFile(path) as zf:
        members = [
            i for i in zf.infolist()
            if not i.is_dir()
            and not i.filename.startswith("__MACOSX/")
            and i.filename.lower().endswith(".csv")
        ]
        if len(members) != 1:
            raise SourceError(f"{path.name}: expected 1 CSV inside, found {[m.filename for m in members]}")
        work_dir.mkdir(parents=True, exist_ok=True)
        out = work_dir / (path.name.split(".", 1)[0] + ".csv")
        with zf.open(members[0]) as src, open(out, "wb") as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
    return out
