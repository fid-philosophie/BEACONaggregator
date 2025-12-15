# download_beacon_file.py

import os
import time
import gzip
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, unquote
from typing import Tuple, Optional, Dict, Any

import requests


def DownloadBeaconFile(
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
