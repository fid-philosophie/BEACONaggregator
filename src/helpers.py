import re
from pathlib import Path
import duckdb

def make_safe_prefix(url: str, max_len: int = 80) -> str:
    """
    Convert an arbitrary URL into a filename-safe prefix.
    - Replace non-alphanumeric characters with '_'
    - Collapse multiple '_' into one
    - Trim to max_len
    - Ensure it ends with '_' (if not empty)
    """
    # Replace non-alnum with '_'
    prefix = re.sub(r"[^A-Za-z0-9]+", "_", url)

    # Collapse multiple underscores
    prefix = re.sub(r"_+", "_", prefix).strip("_")

    if not prefix:
        return ""

    # Trim
    if len(prefix) > max_len:
        prefix = prefix[:max_len].rstrip("_")

    # Ensure trailing underscore to separate from basename
    return prefix + "_"


def analyze_json_path_uniqueness(
    metadata_json: str | Path,
    show_duplicates: bool = True,
    max_examples: int = 10,
) -> dict:
    """
    Analyze uniqueness of 'path' field in a JSON metadata file.

    Returns a dict with:
      - total_items
      - distinct_paths
      - duplicate_items
      - is_unique
      - duplicate_examples (optional)
    """
    metadata_json = Path(metadata_json)

    if not metadata_json.exists():
        raise FileNotFoundError(metadata_json)

    con = duckdb.connect()

    total_items = con.execute(
        "SELECT COUNT(*) FROM read_json_auto($1, format='auto')",
        [str(metadata_json)],
    ).fetchone()[0]

    distinct_paths = con.execute(
        "SELECT COUNT(DISTINCT path) FROM read_json_auto($1, format='auto')",
        [str(metadata_json)],
    ).fetchone()[0]

    duplicate_items = total_items - distinct_paths
    is_unique = duplicate_items == 0

    duplicate_examples = []
    if show_duplicates and not is_unique:
        duplicate_examples = con.execute(
            """
            SELECT path, COUNT(*) AS occurrences
            FROM read_json_auto($1, format='auto')
            GROUP BY path
            HAVING COUNT(*) > 1
            ORDER BY occurrences DESC
            LIMIT ?
            """,
            [str(metadata_json), max_examples],
        ).fetchall()

    con.close()

    # Pretty output
    print(f"\nJSON file: {metadata_json}")
    print(f"Total items:        {total_items:,}")
    print(f"Distinct paths:     {distinct_paths:,}")
    print(f"Duplicate items:    {duplicate_items:,}")
    print(f"Path is unique:     {is_unique}")

    if duplicate_examples:
        print("\nDuplicate path examples:")
        for path, cnt in duplicate_examples:
            print(f"  {cnt}× {path}")

    return {
        "file": str(metadata_json),
        "total_items": total_items,
        "distinct_paths": distinct_paths,
        "duplicate_items": duplicate_items,
        "is_unique": is_unique,
        "duplicate_examples": duplicate_examples,
    }