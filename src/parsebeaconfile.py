# parse_beacon_file.py

from pathlib import Path
from typing import Dict, Optional, List

import pandas as pd


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
