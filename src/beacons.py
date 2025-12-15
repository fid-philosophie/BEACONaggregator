from helpers import make_safe_prefix


import os
import time
import gzip
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, unquote
from typing import Tuple, Optional, Dict, Any

import requests

import json
import pandas as pd

from typing import Optional, Sequence, Any, Dict, Union, List

RAW_URL = (
    "https://raw.githubusercontent.com/"
    "fid-philosophie/BEACONlist/main/latest/BEACONlist.json"
)

def load_beacon_list():
    resp = requests.get(RAW_URL, timeout=30)
    resp.raise_for_status()  # raises if e.g. 404 / 403
    data = resp.json()       # assuming it's valid JSON
    return data


def download_beacon_file(
    url: str,
    out_dir: str | os.PathLike = "data/beacons",
    overwrite: bool = False,
    max_retries: int = 3,
    backoff_factor: float = 1.0,
    request_delay: float = 0.5,
    user_agent: str = "BEACON-downloader/0.1b (nils.geissler@uni-koeln.de)",
    filename_prefix: str = "",
) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    """
    Download a single BEACON file from `url` into `out_dir`.

    Returns:
        (dest_path, metadata_dict) on success or skip,
        (None, None) on hard failure.

    metadata_dict contains:
        - url
        - filename
        - path
        - size_bytes
        - download_time_utc
        - status  ("downloaded" or "skipped_existing")
    """

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # -----------------------------
    # Derive initial basename from URL
    # -----------------------------
    parsed = urlparse(url)
    basename = os.path.basename(parsed.path.rstrip("/"))
    basename = unquote(basename)

    if not basename:                # URL ends with '/'
        basename = "beacon.txt"
    elif "." not in basename:       # no file extension
        basename = f"{basename}.txt"

    # Avoid path traversal or weird characters
    basename = basename.replace(os.sep, "_")

    # Apply prefix
    if filename_prefix:
        basename = f"{filename_prefix}{basename}"

    headers = {"User-Agent": user_agent}

    content: bytes | None = None
    response_headers: dict | None = None

    # -----------------------------
    # Retry loop
    # -----------------------------
    for attempt in range(max_retries + 1):
        try:
            print(f"Downloading {url} (attempt {attempt + 1}/{max_retries + 1})")
            with requests.get(url, stream=True, timeout=30, headers=headers) as r:
                r.raise_for_status()
                response_headers = r.headers

                chunks = []
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        chunks.append(chunk)
                content = b"".join(chunks)

            # success
            break

        except requests.RequestException as e:
            if attempt == max_retries:
                print(f"ERROR downloading {url}: {e}")
                if request_delay > 0:
                    time.sleep(request_delay)
                return None, None

            delay = backoff_factor * (2 ** attempt)
            print(f"Error downloading {url}: {e} — retrying in {delay:.1f}s")
            time.sleep(delay)

    if content is None:
        print(f"ERROR: No content for {url}")
        if request_delay > 0:
            time.sleep(request_delay)
        return None, None

    # -----------------------------
    # Detect gzip & decompress
    # -----------------------------
    is_gz = False

    # 1) URL/path suffix
    if parsed.path.lower().endswith(".gz"):
        is_gz = True

    # 2) Response headers
    if response_headers:
        enc = response_headers.get("Content-Encoding", "").lower()
        typ = response_headers.get("Content-Type", "").lower()
        if "gzip" in enc or "gzip" in typ:
            is_gz = True

    if is_gz:
        try:
            decompressed = gzip.decompress(content)
            content = decompressed

            # Drop .gz extension if present *after prefix was applied*
            if basename.lower().endswith(".gz"):
                basename = basename[:-3] or "beacon.txt"

            print(f"Decompressed gzip content from {url}")
        except OSError:
            print(f"Warning: expected gzip for {url} but could not decompress; saving raw bytes")

    dest_path = out_dir / basename

    # -----------------------------
    # Save file / or skip
    # -----------------------------
    status: str

    if dest_path.exists() and not overwrite:
        print(f"{url} -> {dest_path} (already exists, skipping)")
        size_bytes = dest_path.stat().st_size
        status = "skipped_existing"
    else:
        with dest_path.open("wb") as f:
            f.write(content)
        print(f"Saved {url} -> {dest_path}")
        size_bytes = len(content)
        status = "downloaded"

    download_time = datetime.now(timezone.utc).isoformat()

    metadata: Dict[str, Any] = {
        "url": url,
        "filename": dest_path.name,
        "path": str(dest_path),
        "size_bytes": size_bytes,
        "download_time_utc": download_time,
        "status": status,
    }

    # Be gentle to servers
    if request_delay > 0:
        time.sleep(request_delay)

    return dest_path, metadata




def download_from_beaconlist():
    """ this will take some time and will take up over 270 MB """
    
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir = Path(f"data/beacons_{timestamp}")

    beacon_list = load_beacon_list()
    print("Loaded beacon list:", len(beacon_list), "entries")

    all_metadata = []

    for idx, entry in enumerate(beacon_list):
        url = entry.get("beacon_url")
        if not url:
            print(f"[{idx}] No beacon_url in entry, skipping")
            continue

        # Build a filename-safe prefix from the URL
        url_prefix = make_safe_prefix(url)
        # Add index in front to ensure uniqueness & stable ordering
        prefix = f"{idx:04d}_{url_prefix}" if url_prefix else f"{idx:04d}_"

        dest_path, metadata = download_beacon_file(
            url,
            out_dir=out_dir,
            overwrite=False,
            filename_prefix=prefix,
        )

        if metadata is not None:
            all_metadata.append(metadata)

    # Write one JSON file with all metadata
    metadata_path = out_dir / "beacon_downloads_metadata.json"
    with metadata_path.open("w", encoding="utf-8") as f:
        json.dump(all_metadata, f, ensure_ascii=False, indent=2)

    print(f"Wrote metadata for {len(all_metadata)} entries to {metadata_path}")




def parse_beacon_file(path: str | Path) -> pd.DataFrame:
    """
    Parse a local BEACON file into a pandas DataFrame.

    Extracts these header fields (if present, non-empty):
      - TARGET
      - NAME
      - FEED
      - TIMESTAMP

    Data lines (links) are parsed into up to 9 columns:
      - col1 ... col9

    Columns in the resulting DataFrame:
      - source_file
      - TARGET, NAME, FEED, TIMESTAMP
      - col1, col2, col3, col4, col5, col6, col7, col8, col9
    """

    path = Path(path)

    # header fields we care about (default None)
    header_fields: Dict[str, Optional[str]] = {
        "TARGET": None,
        "NAME": None,
        "FEED": None,
        "TIMESTAMP": None,
    }

    rows: List[Dict[str, Optional[str]]] = []

    MAX_COLS = 9  # <-- now 9 columns

    # utf-8-sig will gracefully handle BOM if present
    # errors="replace" avoids hard crashes on weird encoding bytes
    with path.open("r", encoding="utf-8-sig", errors="replace") as f:
        in_header = True

        for raw_line in f:
            line = raw_line.rstrip("\r\n")

            if in_header:
                # header lines start with '#'
                if line.startswith("#"):
                    stripped = line[1:].strip()
                    if not stripped:
                        # empty header comment
                        continue
                    # Try KEY: value
                    key, sep, value = stripped.partition(":")
                    if not sep:
                        # no ":", not a key-value header we care about
                        continue
                    key = key.strip().upper()
                    value = value.strip()
                    if key in header_fields and value:
                        header_fields[key] = value
                    continue
                # blank lines inside header -> skip
                if not line.strip():
                    continue
                # first non-# non-empty line -> start of data
                in_header = False

            # from here on: data section
            if not line.strip():
                # skip empty data lines
                continue

            parts = line.split("|")
            if len(parts) > MAX_COLS:
                print(
                    f"[WARN] {path} line has {len(parts)} columns (>{MAX_COLS}); "
                    f"truncating to first {MAX_COLS}."
                )
                parts = parts[:MAX_COLS]

            # build row dict
            row: Dict[str, Optional[str]] = {
                "source_file": str(path),
                "TARGET": header_fields["TARGET"],
                "NAME": header_fields["NAME"],
                "FEED": header_fields["FEED"],
                "TIMESTAMP": header_fields["TIMESTAMP"],
            }

            for i in range(MAX_COLS):
                if i < len(parts):
                    row[f"col{i+1}"] = parts[i].strip()
                else:
                    row[f"col{i+1}"] = None

            rows.append(row)

    df = pd.DataFrame(
        rows,
        columns=[
            "source_file",
            "TARGET",
            "NAME",
            "FEED",
            "TIMESTAMP",
            "col1",
            "col2",
            "col3",
            "col4",
            "col5",
            "col6",
            "col7",
            "col8",
            "col9",
        ],
    )

    return df
