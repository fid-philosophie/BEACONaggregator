# main.py

#from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from load_beacon_list import LoadBeaconList
from download_beacon_file import DownloadBeaconFile
from parsebeaconfile import parse_beacon_file

import pyarrow.parquet as pq

import duckdb

from datetime import datetime


import gc

from dataclasses import dataclass
from typing import Optional, Sequence, Any, Dict, Union, List

import duckdb
import pandas as pd

from collections import defaultdict


@dataclass
class ParquetInspection:
    path_repr: str
    schema: list[tuple[str, str]]
    row_count: Optional[int]
    preview_df: pd.DataFrame
    random_sample_df: pd.DataFrame
    file_metadata: Optional[pd.DataFrame]
    null_counts: Optional[pd.DataFrame]
    approx_distinct: Optional[pd.DataFrame]
    min_max: Optional[pd.DataFrame]




def add_contains_matches_and_dump_json(
    df: pd.DataFrame,
    parquet_paths: list[str | Path],
    out_dir: str | Path = "data/beaconlist",
    out_name: str | None = None,
    batch_size: int = 200_000,
    regex_chunk_size: int = 500,
) -> Path:
    """
    - Adds df['safe_prefix'] = make_safe_prefix(df['beacon_url'])
    - Adds df['matched_parquets'] = list of parquet paths where ANY source_file contains safe_prefix
    - Writes df to JSON in out_dir

    RAM-friendly: reads only 'source_file' column in Arrow batches.
    """
    import pyarrow.parquet as pq

    if "beacon_url" not in df.columns:
        raise KeyError("df must contain column 'beacon_url'")

    df = df.copy()

    # 1) sanitize
    df["safe_prefix"] = df["beacon_url"].map(
        lambda x: make_safe_prefix(x) if pd.notna(x) else pd.NA
    )

    prefixes = (
        df["safe_prefix"]
        .dropna()
        .astype(str)
        .map(str.strip)
        .loc[lambda s: s != ""]
        .unique()
        .tolist()
    )

    # Nothing to match
    if not prefixes:
        df["matched_parquets"] = [[] for _ in range(len(df))]
        return _dump_df_json(df, out_dir=out_dir, out_name=out_name)

    # 2) build chunked regexes (literal substring match)
    # NOTE: re.escape so prefixes are treated as plain text
    compiled: list[re.Pattern] = []
    for i in range(0, len(prefixes), regex_chunk_size):
        chunk = prefixes[i : i + regex_chunk_size]
        pat = "(" + "|".join(re.escape(p) for p in chunk) + ")"
        compiled.append(re.compile(pat))

    # prefix -> list of parquet files where it was seen
    found_in: dict[str, list[str]] = defaultdict(list)

    # 3) scan parquets in batches
    for p in map(Path, parquet_paths):
        p = p.resolve()
        if not p.exists():
            print(f"Skipping missing parquet: {p}")
            continue

        print(f"Scanning {p} ...")
        pf = pq.ParquetFile(p)

        # prefix values already credited for this parquet file
        seen_this_file: set[str] = set()

        for batch in pf.iter_batches(columns=["source_file"], batch_size=batch_size):
            values = batch.column(0).to_pylist()

            for s in values:
                if s is None:
                    continue

                # run each regex chunk; record all matched prefixes
                for rx in compiled:
                    m = rx.search(s)
                    if not m:
                        continue

                    # There might be multiple matches from the same rx in a string
                    # (e.g. multiple prefixes). Find them all.
                    for mm in rx.finditer(s):
                        pref = mm.group(0)
                        if pref not in seen_this_file:
                            found_in[pref].append(str(p))
                            seen_this_file.add(pref)

    # 4) write matches back to df (list of parquet paths)
    df["matched_parquets"] = df["safe_prefix"].map(
        lambda v: found_in.get(str(v), []) if pd.notna(v) else []
    )

    # 5) dump JSON
    return _dump_df_json(df, out_dir=out_dir, out_name=out_name)


def _dump_df_json(df: pd.DataFrame, out_dir: str | Path, out_name: str | None) -> Path:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if out_name is None:
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        out_name = f"beacon_parquet_contains_matches_{ts}.json"

    out_path = out_dir / out_name
    df.to_json(out_path, orient="records", indent=2, force_ascii=False)
    print(f"Wrote JSON: {out_path}")
    return out_path


def qident(name: str) -> str:
    """Quote a SQL identifier (column/table name) for DuckDB."""
    return '"' + name.replace('"', '""') + '"'


def inspect_parquet_with_duckdb(
    parquet_path: Union[str, Path, Sequence[Union[str, Path]]],
    *,
    preview_rows: int = 10,
    random_sample_rows: int = 100,
    sample_seed: Optional[int] = None,     # reproducible if set
    compute_row_count: bool = True,
    stats_mode: str = "sample",            # "none" | "sample" | "full"
    sample_rows_for_stats: int = 200_000,
    columns_for_distinct: Optional[Sequence[str]] = None,
    include_file_metadata: bool = True,    # best-effort (most reliable for single path / glob)
) -> ParquetInspection:
    """
    DuckDB 1.4.2-compatible Parquet inspector.

    Shows:
      1) column names + indexes
      2) column types
      3) useful extras: preview, row count, file metadata (best-effort),
         null counts, approx distinct, min/max (optional)

    Returns:
      - random_sample_df: random sample rows as a pandas DataFrame
    """

    if stats_mode not in {"none", "sample", "full"}:
        raise ValueError("stats_mode must be one of: 'none', 'sample', 'full'")

    def _to_str(p: Union[str, Path]) -> str:
        return p.as_posix() if isinstance(p, Path) else str(p)

    def qident(name: str) -> str:
        """Quote a SQL identifier (column/table name) for DuckDB."""
        return '"' + name.replace('"', '""') + '"'

    # Normalize paths / arguments for read_parquet(?)
    if isinstance(parquet_path, (list, tuple)):
        paths: List[str] = [_to_str(p) for p in parquet_path]
        read_parquet_arg: Union[str, List[str]] = paths
        path_repr = ", ".join(paths[:3]) + (" ..." if len(paths) > 3 else "")
        single_for_metadata: Optional[str] = None
    else:
        p = _to_str(parquet_path)
        read_parquet_arg = p
        path_repr = p
        single_for_metadata = p

    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA enable_progress_bar=false;")

    # 1) + 2) schema (names + types)
    describe_rows = con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?);",
        [read_parquet_arg],
    ).fetchall()
    schema = [(r[0], r[1]) for r in describe_rows]

    print("\nColumns (index: name : type)")
    for i, (name, typ) in enumerate(schema):
        print(f"  {i:>3}: {name} : {typ}")

    # Best-effort file metadata
    file_metadata_df: Optional[pd.DataFrame] = None
    if include_file_metadata and single_for_metadata is not None:
        try:
            file_metadata_df = con.execute(
                "SELECT * FROM parquet_file_metadata(?);",
                [single_for_metadata],
            ).fetchdf()

            print("\nParquet file metadata (high level):")
            cols = [c for c in ["file_name", "num_rows", "num_row_groups", "created_by", "format_version"] if c in file_metadata_df.columns]
            print(file_metadata_df[cols] if cols else file_metadata_df.head(5))
        except duckdb.Error:
            file_metadata_df = None

    # Preview
    preview_df = con.execute(
        f"SELECT * FROM read_parquet(?) LIMIT {int(preview_rows)};",
        [read_parquet_arg],
    ).fetchdf()
    print(f"\nPreview (first {preview_rows} rows):")
    print(preview_df)

    # Random sample (DuckDB 1.4.2-compatible): ORDER BY random()
    if sample_seed is not None:
        # setseed expects float in [0,1); map int -> float deterministically
        con.execute("SELECT setseed(?);", [((int(sample_seed) % 1_000_000) / 1_000_000.0)])

    random_sample_df = con.execute(
        f"""
        SELECT *
        FROM read_parquet(?)
        ORDER BY random()
        LIMIT {int(random_sample_rows)};
        """,
        [read_parquet_arg],
    ).fetchdf()
    print(f"\nRandom sample ({random_sample_rows} rows):")
    print(random_sample_df)

    # Row count
    row_count: Optional[int] = None
    if compute_row_count:
        row_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?);",
            [read_parquet_arg],
        ).fetchone()[0]
        print(f"\nRow count: {row_count:,}")

    # Optional stats
    null_counts_df = approx_distinct_df = min_max_df = None
    if stats_mode != "none":
        if stats_mode == "full":
            rel_sql = "SELECT * FROM read_parquet(?)"
            rel_params = [read_parquet_arg]
        else:
            rel_sql = f"SELECT * FROM read_parquet(?) LIMIT {int(sample_rows_for_stats)}"
            rel_params = [read_parquet_arg]

        # Null counts
        null_exprs = ", ".join(
            f"SUM(CASE WHEN {qident(col)} IS NULL THEN 1 ELSE 0 END) AS {qident(col)}"
            for col, _ in schema
        )
        null_counts_df = con.execute(
            f"SELECT {null_exprs} FROM ({rel_sql}) t;",
            rel_params,
        ).fetchdf()

        # Approx distinct
        cols_for_dist = list(columns_for_distinct) if columns_for_distinct else [c for c, _ in schema]
        dist_exprs = ", ".join(
            f"approx_count_distinct({qident(col)}) AS {qident(col)}"
            for col in cols_for_dist
        )
        approx_distinct_df = con.execute(
            f"SELECT {dist_exprs} FROM ({rel_sql}) t;",
            rel_params,
        ).fetchdf()

        # Min/Max (best effort per column)
        rows: list[tuple[str, Any, Any]] = []
        for col, _typ in schema:
            col_id = qident(col)
            try:
                mn, mx = con.execute(
                    f"SELECT MIN({col_id}) AS mn, MAX({col_id}) AS mx FROM ({rel_sql}) t;",
                    rel_params,
                ).fetchone()
                rows.append((col, mn, mx))
            except duckdb.Error:
                pass
        min_max_df = pd.DataFrame(rows, columns=["column", "min", "max"])

    con.close()

    return ParquetInspection(
        path_repr=path_repr,
        schema=schema,
        row_count=row_count,
        preview_df=preview_df,
        random_sample_df=random_sample_df,
        file_metadata=file_metadata_df,
        null_counts=null_counts_df,
        approx_distinct=approx_distinct_df,
        min_max=min_max_df,
    )

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


def download_from_beaconlist():
    """ this will take some time and will take up over 270 MB """
    
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir = Path(f"data/beacons_{timestamp}")

    beacon_list = LoadBeaconList()
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

        dest_path, metadata = DownloadBeaconFile(
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

def collect_beacons_to_dataframe(
    beacons_dir: str | Path,
    skip_suffixes: tuple[str, ...] = (".json",),
) -> pd.DataFrame:
    """
    Read all BEACON files in `beacons_dir` into a single DataFrame.

    Skips files whose suffix is in skip_suffixes (e.g. the downloads metadata JSON).
    """

    beacons_dir = Path(beacons_dir)

    dfs = []

    for path in sorted(beacons_dir.iterdir()):
        if not path.is_file():
            continue
        if path.suffix in skip_suffixes:
            # skip metadata / non-BEACONs
            continue

        print(f"Parsing {path} ...")
        df = parse_beacon_file(path)
        if df.empty:
            print(f"  -> no data rows found in {path}")
        else:
            print(f"  -> parsed {len(df)} rows")
            dfs.append(df)

    # if not dfs:
    #     print("No BEACON data parsed.")
    #     return pd.DataFrame(
    #         columns=[
    #             "source_file",
    #             "TARGET",
    #             "NAME",
    #             "FEED",
    #             "TIMESTAMP",
    #             "col1",
    #             "col2",
    #             "col3",
    #             "col4",
    #             "col5"
    #         ]
    #     )
    
    if not dfs:
        print("No BEACON data parsed.")
        return pd.DataFrame(
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
                "col5"
            ]
        )



    big_df = pd.concat(dfs, ignore_index=True)
    print(f"Total rows combined: {len(big_df)}")
    return big_df

def beacons_to_parquet__OLD():
    """ takes a while """
    beacons_dir = Path("data/beacons")
    out_dir = Path("data/aggregations")
    beacons_dir.mkdir(parents=True, exist_ok=True)

    # Build one big DataFrame from all BEACON files
    df = collect_beacons_to_dataframe(beacons_dir)

    # --- Write Parquet ---
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    parquet_path = out_dir / f"beacons_{timestamp}.parquet"
    # requires pyarrow or fastparquet
    df.to_parquet(parquet_path, index=False)
    print(f"Wrote Parquet: {parquet_path}")
    return parquet_path


EXPECTED_COLUMNS = [
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
]


def _normalize_beacon_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make schema stable across all chunks:
    - ensure all expected columns exist
    - keep fixed order
    - coerce to pandas 'string' dtype to avoid Arrow 'null' type inference
    """
    df = df.copy()

    # Add missing columns
    for c in EXPECTED_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA

    # Drop unexpected columns (optional; remove this if you want to keep extras)
    df = df[EXPECTED_COLUMNS]

    # Coerce everything to string dtype (keeps NA as <NA>, not "nan")
    for c in EXPECTED_COLUMNS:
        df[c] = df[c].astype("string")

    return df


def iter_beacon_dataframes(
    beacons_dir: str | Path,
    skip_suffixes: tuple[str, ...] = (".json",),
):
    beacons_dir = Path(beacons_dir)

    for path in sorted(beacons_dir.iterdir()):
        if not path.is_file():
            continue
        if path.suffix in skip_suffixes:
            continue

        print(f"Parsing {path} ...")
        df = parse_beacon_file(path)

        if df.empty:
            print(f"  -> no data rows found in {path}")
            continue

        print(f"  -> parsed {len(df)} rows")
        yield _normalize_beacon_df(df)


def beacons_to_parquet(
    beacons_dir: str | Path = "data/beacons",
    out_dir: str | Path = "data/aggregations",
    skip_suffixes: tuple[str, ...] = (".json",),
) -> Path:
    """
    Stream BEACON files to ONE parquet without holding everything in RAM.

    Requires: pyarrow
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    beacons_dir = Path(beacons_dir)
    out_dir = Path(out_dir)
    beacons_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    parquet_path = out_dir / f"beacons_{timestamp}.parquet"

    # Fixed Arrow schema: all strings (robust against mixed types / missing columns)
    arrow_schema = pa.schema([(c, pa.string()) for c in EXPECTED_COLUMNS])

    writer: pq.ParquetWriter | None = None
    total_rows = 0

    try:
        for df in iter_beacon_dataframes(beacons_dir, skip_suffixes=skip_suffixes):
            # Use explicit schema so we NEVER infer 'null' fields
            table = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(parquet_path, arrow_schema)

            writer.write_table(table)
            total_rows += len(df)

            del table, df
            gc.collect()

    finally:
        if writer is not None:
            writer.close()

    if total_rows == 0:
        print("No BEACON data parsed; nothing written.")
    else:
        print(f"Wrote Parquet: {parquet_path} (rows: {total_rows})")

    return parquet_path

def look_into_parquet(filename):
    # table = pq.ParquetFile(filename)

    # # for batch in table.iter_batches(batch_size=100000):
    # #     df = batch.to_pandas()
    # #     # process df here
    # # df_head = batch.to_pandas()
    # # print(df_head)

    # pf = pq.ParquetFile(filename)

    # print(pf.schema_arrow.names)
    # print(pf.schema_arrow)

    # batch = next(pf.iter_batches(batch_size=5))
    # df_preview = batch.to_pandas()
    # print(df_preview)

    # found = False
    # column_name = "col4"

    # for batch in pf.iter_batches(columns=[column_name], batch_size=100000):
    #     col = batch.column(column_name)
    #     if col.null_count < len(col):    # means at least one non-null
    #         found = True
    #         break

    # print(column_name + " has a non-null value:", found)

    # examples = []
    # N = 20  # how many samples you want

    # for batch in pf.iter_batches(columns=[column_name], batch_size=100000):
    #     col = batch.column(column_name).to_pylist()  # convert only this column
    #     for value in col:
    #         if value is not None:
    #             examples.append(value)
    #             if len(examples) >= N:
    #                 break
    #     if len(examples) >= N:
    #         break

    # print(examples)

    # column_name = "col1"

    # rows = duckdb.sql(f"""
    # SELECT *
    # FROM (
    #     SELECT COUNT(*)
    #     FROM '{filename}'
    #     WHERE {column_name} IS NOT NULL
    #     AND col1 IS NOT NULL
    #     AND col2 IS NOT NULL
    #     AND col3 IS NOT NULL
    #     AND TARGET IS NULL
    # )
    # """).fetchone()[0]
    # print("rows=", rows)

    out_dir = Path("data/aggregations")

    rows = duckdb.sql(f"""
    SELECT *
    FROM (
        SELECT COUNT(*)
        FROM '{filename}'
        WHERE col1 IS NOT NULL
        AND col1 IS NOT NULL
        AND col2 IS NOT NULL
        AND col3 IS NOT NULL
        AND TARGET IS NULL
    )
    """).fetchone()[0]
    print("col1-3 not null + target null")
    print("rows=", rows)

    rows = duckdb.sql(f"""
    SELECT *
    FROM (
        SELECT COUNT(*)
        FROM '{filename}'
        WHERE col1 IS NOT NULL
        AND col1 IS NOT NULL
        AND col2 IS NOT NULL
        AND col3 IS NULL
        AND TARGET IS NULL
    )
    """).fetchone()[0]
    print("col1-2 not null + target null")
    print("rows=", rows)

    rows = duckdb.sql(f"""
    SELECT *
    FROM (
        SELECT COUNT(*)
        FROM '{filename}'
        WHERE col1 IS NOT NULL
        AND col1 IS NOT NULL
        AND col2 IS NULL
        AND col3 IS NULL
        AND TARGET IS NULL
    )
    """).fetchone()[0]
    print("col1 not null + target null")
    print("rows=", rows)


    rows = duckdb.sql(f"""
    SELECT *
    FROM (
        SELECT COUNT(*)
        FROM '{filename}'
        WHERE col1 IS NULL
        AND col2 IS NOT NULL
        AND col3 IS NULL
        AND TARGET IS NULL
    )
    """).fetchone()[0]
    print("col1 null, col2 not null + target null")
    print("rows=", rows)


###########################################
    rows = duckdb.sql(f"""
    SELECT *
    FROM (
        SELECT COUNT(*)
        FROM '{filename}'
        WHERE col1 IS NOT NULL
        AND col1 IS NOT NULL
        AND col2 IS NOT NULL
        AND col3 IS NOT NULL
        AND TARGET IS NOT NULL
    )
    """).fetchone()[0]
    print("col1-3 not null + target not null")
    print("rows=", rows)

    rows = duckdb.sql(f"""
    SELECT *
    FROM (
        SELECT COUNT(*)
        FROM '{filename}'
        WHERE col1 IS NOT NULL
        AND col1 IS NOT NULL
        AND col2 IS NOT NULL
        AND col3 IS NULL
        AND TARGET IS NOT NULL
    )
    """).fetchone()[0]
    print("col1-2 not null + target not null")
    print("rows=", rows)

    rows = duckdb.sql(f"""
    SELECT *
    FROM (
        SELECT COUNT(*)
        FROM '{filename}'
        WHERE col1 IS NOT NULL
        AND col1 IS NOT NULL
        AND col2 IS NULL
        AND col3 IS NULL
        AND TARGET IS NOT NULL
    )
    """).fetchone()[0]
    print("col1 not null + target not null")
    print("rows=", rows)


    rows = duckdb.sql(f"""
    SELECT *
    FROM (
        SELECT COUNT(*)
        FROM '{filename}'
        WHERE col1 IS NULL
        AND col2 IS NOT NULL
        AND col3 IS NULL
        AND TARGET IS NOT NULL
    )
    """).fetchone()[0]
    print("col1 null, col2 not null + target not null")
    print("rows=", rows)
    
    df = duckdb.sql(f"""
    SELECT *
    FROM (
        SELECT COUNT(*)
        FROM '{filename}'
        WHERE col1 IS NULL
        AND col2 IS NOT NULL
        AND col3 IS NULL
        AND TARGET IS NOT NULL
    )
    """).df()
    parquet_path = out_dir / f"target-notnull_col2-only.parquet"
    df.to_parquet(parquet_path, index=False)
    print(f"Wrote Parquet: {parquet_path}")

    

    # # get some examples
    # df = duckdb.sql(f"""
    # SELECT *
    # FROM (
    #     SELECT
    #         *,
    #         row_number() OVER () AS row_id
    #     FROM '{filename}'
    #     WHERE {column_name} IS NOT NULL
    #     AND col1 IS NOT NULL
    #     AND col2 IS NOT NULL
    #     AND col3 IS NOT NULL
    # )
    # USING SAMPLE 100 ROWS;
    # """).df()

    #print(df)
    #df.to_json("df_out.json", orient="records", indent=2)



def count_categories(parquet_path):
    rows = duckdb.sql(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
    print("Row count:", rows)

    df = duckdb.sql(f"""
    SELECT *
    FROM '{parquet_path}'
    WHERE col2 IS NULL
    AND col3 IS NULL
    AND col4 IS NULL
    AND col5 IS NULL
    AND col6 IS NULL
    AND col7 IS NULL
    AND col8 IS NULL
    AND col9 IS NULL
    AND TARGET IS NOT NULL
    AND col1 IS NULL
    ;
    """).df()
    print(df)
    print(df.shape)

    df.to_json("df_out_targetNOTnull_0col.json", orient="records", indent=2)

from pathlib import Path
import duckdb

def split_parquet(filename):
    out_dir = Path("data/split_aggregations")
    out_dir.mkdir(parents=True, exist_ok=True)

    # (suffix, target_condition, col_condition)
    configs = [
        # TARGET IS NULL
        ("target-null_col1-only",   "TARGET IS NULL",     "col1 IS NOT NULL AND col2 IS NULL     AND col3 IS NULL"),
        ("target-null_col1-2",      "TARGET IS NULL",     "col1 IS NOT NULL AND col2 IS NOT NULL AND col3 IS NULL"),
        ("target-null_col1-2-3",    "TARGET IS NULL",     "col1 IS NOT NULL AND col2 IS NOT NULL AND col3 IS NOT NULL"),

        # TARGET IS NOT NULL
        ("target-notnull_col1-only","TARGET IS NOT NULL", "col1 IS NOT NULL AND col2 IS NULL     AND col3 IS NULL"),
        ("target-notnull_col1-2",   "TARGET IS NOT NULL", "col1 IS NOT NULL AND col2 IS NOT NULL AND col3 IS NULL"),
        ("target-notnull_col1-2-3", "TARGET IS NOT NULL", "col1 IS NOT NULL AND col2 IS NOT NULL AND col3 IS NOT NULL"),
    ]

    for suffix, target_cond, col_cond in configs:
        where_clause = f"WHERE {target_cond} AND {col_cond}"

        # 1) Count rows for this slice
        rows = duckdb.sql(
            f"""
            SELECT COUNT(*)
            FROM '{filename}'
            {where_clause}
            """
        ).fetchone()[0]

        print(suffix)
        print("rows =", rows)

        if rows == 0:
            # nothing to write, skip
            continue

        # 2) Write this slice directly to Parquet USING DUCKDB (no pandas)
        parquet_path = out_dir / f"{suffix}.parquet"
        duckdb.sql(
            f"""
            COPY (
                SELECT *
                FROM '{filename}'
                {where_clause}
            ) TO '{parquet_path.as_posix()}'
            (FORMAT 'parquet');
            """
        )
        print(f"Wrote Parquet: {parquet_path}")


from pathlib import Path
import pandas as pd

def sample_parquet_dir(
    dir_path,
    n_rows=100,
    pattern="*.parquet",
    random_state=42
):
    """
    For each parquet file in dir_path matching pattern:
      - read it into a DataFrame
      - take up to n_rows random rows
      - write them to a JSON file with the same name (but .json extension)

    Returns
    -------
    dict[str, pd.DataFrame]
        A dict mapping base filename -> sampled DataFrame.
    """
    dir_path = Path(dir_path)
    if not dir_path.is_dir():
        raise NotADirectoryError(f"{dir_path} is not a directory")

    samples = {}

    for parquet_path in dir_path.glob(pattern):
        print(f"Processing {parquet_path.name}...")
        # Read parquet
        df = pd.read_parquet(parquet_path)

        # Take up to n_rows rows (random sample if larger, otherwise all rows)
        if len(df) > n_rows:
            df_sample = df.sample(n=n_rows, random_state=random_state)
        else:
            df_sample = df

        # Save to JSON with same base name
        json_path = parquet_path.with_suffix(".json")
        df_sample.to_json(json_path, orient="records", lines=False, indent=2)

        samples[parquet_path.name] = df_sample

    return samples


from pathlib import Path
import duckdb

def OLD_create_resolved_parquets(parquet_paths):
    #parquet_dir = Path(parquet_dir)

    #for path in parquet_dir.glob("*.parquet"):
    for path in parquet_paths:
        #name = path.name
        actualpath = Path(path)
        name = actualpath.name

        # Decide resolved expression based on filename patterns
        if "target-notnull" in name and "col1-2-3" in name:
            # concat TARGET and col3
            #resolved_expr = "TARGET || ' ' || col3"
            value_expr = "col3"
        elif "target-notnull" in name and "col1-2" in name:
            # concat TARGET and col1
            #resolved_expr = "TARGET || ' ' || col1"
            value_expr = "col1"
        elif "target-notnull" in name and "col1-only" in name:
            # concat TARGET and col1
            #resolved_expr = "TARGET || ' ' || col1"
            value_expr = "col1"
        elif "target-null" in name and "col1-2-3" in name:
            # take col3
            resolved_expr = "col3"
        elif "target-null" in name and "col1-2" in name:
            # take col2
            resolved_expr = "col2"
        else:
            print(f"Skipping {name}: no matching rule for filename")
            continue


        # Build resolved_expr for target-notnull cases
        if "target-notnull" in name:
            value_expr = f"""
                CASE
                    WHEN TARGET LIKE '%{{ID}}%'
                        THEN REPLACE(TARGET, '{{ID}}', {value_expr})
                    ELSE TARGET || ' ' || {value_expr}
                END
            """


        out_path = actualpath.with_stem(actualpath.stem + "_resolved")

        print(f"Creating {out_path.name} (rule from {name})")

        duckdb.sql(f"""
            COPY (
                SELECT
                    source_file,
                    TARGET,
                    NAME,
                    FEED,
                    TIMESTAMP,
                    col1,
                    col2,
                    col3,
                    col4,
                    col5,
                    {resolved_expr} AS resolved
                FROM '{actualpath.as_posix()}'
            ) TO '{out_path.as_posix()}'
            (FORMAT 'parquet');
        """)

        print(f"  → wrote {out_path}")


def create_resolved_parquets(parquet_paths):
    for path in parquet_paths:
        actualpath = Path(path)
        name = actualpath.name

        resolved_expr = None  # IMPORTANT: reset every loop iteration

        # Decide which column provides the ID/value
        if "target-notnull" in name and "col1-2-3" in name:
            id_col = "col3"
        elif "target-notnull" in name and "col1-2" in name:
            id_col = "col1"
        elif "target-notnull" in name and "col1-only" in name:
            id_col = "col1"
        elif "target-null" in name and "col1-2-3" in name:
            resolved_expr = "col3"
        elif "target-null" in name and "col1-2" in name:
            resolved_expr = "col2"
        else:
            print(f"Skipping {name}: no matching rule for filename")
            continue

        # Build resolved_expr for target-notnull cases
        if "target-notnull" in name:
            resolved_expr = f"""
                CASE
                    WHEN TARGET LIKE '%{{ID}}%'
                        THEN REPLACE(TARGET, '{{ID}}', CAST({id_col} AS VARCHAR))
                    ELSE TARGET || ' ' || CAST({id_col} AS VARCHAR)
                END
            """

        out_path = actualpath.with_stem(actualpath.stem + "_resolved")
        print(f"Creating {out_path.name} (rule from {name})")

        duckdb.sql(f"""
            COPY (
                SELECT
                    source_file,
                    TARGET,
                    NAME,
                    FEED,
                    TIMESTAMP,
                    col1,
                    col2,
                    col3,
                    col4,
                    col5,
                    {resolved_expr} AS resolved
                FROM '{actualpath.as_posix()}'
            ) TO '{out_path.as_posix()}'
            (FORMAT 'parquet');
        """)

        print(f"  → wrote {out_path}")


def dump_df_samples_to_json(
    df: pd.DataFrame,
    parquet_file: str | Path,
    out_dir: str | Path | None = None,
) -> Path:
    """
    Dump DataFrame to JSON.
    The JSON filename is derived from `parquet_file`,
    replacing `.parquet` with `_samples.json`.
    """
    parquet_file = Path(parquet_file)

    if parquet_file.suffix != ".parquet":
        raise ValueError("Expected a .parquet file name")

    json_name = parquet_file.with_suffix("").name + "_samples.json"

    if out_dir is None:
        json_path = parquet_file.with_name(json_name)
    else:
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        json_path = out_dir / json_name

    # Write JSON
    df.to_json(
        json_path,
        orient="records",   # list of objects
        indent=2,           # pretty-print
        force_ascii=False,  # keep UTF-8
    )

    return json_path

def make_inspectable_jsons_from_parquets(parquet_paths):

    for parquet_path in parquet_paths:
        #make inspectable json files (with samples) from single parquets
        info = inspect_parquet_with_duckdb(
            Path(parquet_path),
            preview_rows=10,
            random_sample_rows=100,
            sample_seed=42,          # optional, for reproducible sampling
            compute_row_count=True,
            stats_mode="sample",     # "none" | "sample" | "full"
        )

        df_sample = info.random_sample_df
        print(parquet_path)
        print(df_sample)

        json_path = dump_df_samples_to_json(
            df_sample,
            parquet_path,
        )
        print(f"Wrote JSON samples: {json_path}")

def main() -> None:
    data_dir = Path("data")
    parquet_path = data_dir / "aggregations" / "beacons_20251212-1605.parquet"


    # use parquet_path for sample creation (=> inspect json files)
    #parquet_path = Path("data/split_aggregations/target-null_col1-only.parquet")
    #parquet_path = Path("data/split_aggregations/target-null_col1-2.parquet")
    #parquet_path = Path("data/split_aggregations/target-null_col1-2-3.parquet")

    #parquet_path = Path("data/split_aggregations/target-notnull_col1-only.parquet")
    #parquet_path = Path("data/split_aggregations/target-notnull_col1-2.parquet")
    #parquet_path = Path("data/split_aggregations/target-notnull_col1-2-3.parquet")


    # use parquet_path for sample creation (=> inspect json files) // same same, but for resolved urls
    #---doesnotexist---parquet_path = Path("data/split_aggregations/target-null_col1-only_resolved.parquet")
    #parquet_path = Path("data/split_aggregations/target-null_col1-2_resolved.parquet")
    #parquet_path = Path("data/split_aggregations/target-null_col1-2-3_resolved.parquet")

    #parquet_path = Path("data/split_aggregations/target-notnull_col1-only_resolved.parquet")
    #parquet_path = Path("data/split_aggregations/target-notnull_col1-2_resolved.parquet")
    #parquet_path = Path("data/split_aggregations/target-notnull_col1-2-3_resolved.parquet")


    parquet_toresolveurls_paths = [
        "data/split_aggregations/target-null_col1-2.parquet",
        "data/split_aggregations/target-null_col1-2-3.parquet",
        "data/split_aggregations/target-notnull_col1-only.parquet",
        "data/split_aggregations/target-notnull_col1-2.parquet",
        "data/split_aggregations/target-notnull_col1-2-3.parquet",
    ]

    parquet_withresolvedurls_paths = [
        "data/split_aggregations/target-null_col1-2_resolved.parquet",
        "data/split_aggregations/target-null_col1-2-3_resolved.parquet",
        "data/split_aggregations/target-notnull_col1-only_resolved.parquet",
        "data/split_aggregations/target-notnull_col1-2_resolved.parquet",
        "data/split_aggregations/target-notnull_col1-2-3_resolved.parquet",
    ]


    parquet_dir = data_dir / "split_aggregations"

    #parquet_path = beacons_to_parquet() # beacons in 1 parquet umwandeln

    #print(parquet_path)

    #look_into_parquet(parquet_path) # parquet auswerten

    #split_parquet(parquet_path) # split by target null/not null and col1-3

    # Point this to your directory with parquet files
    #samples = sample_parquet_dir(parquet_dir)

    # `samples` is now a dict of DataFrames for quick exploration in Python:
    # e.g. see the sample for one file:
    #samples["my_file.parquet"].head()

    # resolve urls for all types of beacons and create new parquets:
    #create_resolved_parquets(parquet_dir)
    create_resolved_parquets(parquet_toresolveurls_paths)


    # rows = duckdb.sql(f"""
    # SELECT COUNT(*)
    # FROM '{parquet_path}'
    # WHERE col2 IS NOT NULL
    #   AND col3 IS NULL
    #   AND col4 IS NULL
    #   AND col5 IS NULL
    #   AND col6 IS NULL
    #   AND col7 IS NULL
    #   AND col8 IS NULL
    #   AND col9 IS NULL
    #   AND TARGET IS NOT NULL
    # """).fetchone()[0]

    # print("Number of matching rows:", rows)

    # info = inspect_parquet_with_duckdb(
    #     parquet_path,
    #     random_sample_rows=100,
    #     sample_seed=42,          # omit for non-deterministic
    #     stats_mode="sample",
    # )
    # df = info.random_sample_df


    
    # make inspectable json files (with samples) from single parquets
    make_inspectable_jsons_from_parquets(parquet_withresolvedurls_paths)

    # info = inspect_parquet_with_duckdb(
    #     Path(parquet_path),
    #     preview_rows=10,
    #     random_sample_rows=100,
    #     sample_seed=42,          # optional, for reproducible sampling
    #     compute_row_count=True,
    #     stats_mode="sample",     # "none" | "sample" | "full"
    # )

    # df_sample = info.random_sample_df
    # print(df_sample)

    # json_path = dump_df_samples_to_json(
    #     df_sample,
    #     parquet_path,
    # )
    # print(f"Wrote JSON samples: {json_path}")


    beaconlist_json = LoadBeaconList()

    df_beaconlist = pd.DataFrame(beaconlist_json)



    out_path = add_contains_matches_and_dump_json(df_beaconlist, parquet_withresolvedurls_paths)

    print(out_path)



    # Hagrid NDIF fields
    # target_uri, normdata_uri, type, role, label, project, source_date, date_of_export
    # wikidata/gnd?
    # feed/beacon_url?
    # ?
    # target id ?
    # authority id ? / source/reference/authority id?!




if __name__ == "__main__":
    main()
