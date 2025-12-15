import requests

def LoadBeaconFile(url: str):
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    # Try to guess encoding; fall back to UTF-8
    if r.encoding is None:
        r.encoding = r.apparent_encoding or "utf-8"

    header_lines: list[str] = []
    data_lines: list[str] = []

    in_header = True
    for line in r.text.splitlines():
        if in_header and line.startswith("#"):
            header_lines.append(line)
        elif in_header and not line.startswith("#"):
            # first non-# line → from here on it's data
            in_header = False
            if line.strip():          # skip empty lines
                data_lines.append(line)
        else:
            if line.strip():
                data_lines.append(line)

    # Parse data lines: keep raw line + parts split by '|'
    records = []
    for line in data_lines:
        parts = line.split("|")
        records.append({
            "raw": line,   # whole original line
            "parts": parts # list of fields, length can vary
        })

    return header_lines, records
