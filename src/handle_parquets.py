from __future__ import annotations

import pyarrow as pa

import pyarrow.parquet as pq
from pathlib import Path
from typing import Optional, Sequence, Any, Dict, Union, List, Iterable, Sequence
from dataclasses import dataclass
import pandas as pd
import duckdb

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


from pathlib import Path
import duckdb


def count_parquet_rows(parquet_path: str | Path) -> int:
    """
    Count rows in a parquet file using DuckDB (no pandas).
    """
    parquet_path = Path(parquet_path)

    if not parquet_path.exists():
        raise FileNotFoundError(parquet_path)

    con = duckdb.connect()

    row_count = con.execute(
        "SELECT COUNT(*) FROM read_parquet($1)",
        [str(parquet_path)],
    ).fetchone()[0]

    con.close()
    return row_count


def compare_parquet_row_counts(
    before_parquet: str | Path,
    after_parquet: str | Path,
    strict: bool = False,
) -> dict:
    """
    Compare row counts of two parquet files using DuckDB.

    Args:
        before_parquet: original parquet file
        after_parquet:  parquet after transformation
        strict: if True, raise an error when counts differ

    Returns:
        dict with row counts and difference
    """
    before_parquet = Path(before_parquet)
    after_parquet = Path(after_parquet)

    if not before_parquet.exists():
        raise FileNotFoundError(before_parquet)
    if not after_parquet.exists():
        raise FileNotFoundError(after_parquet)

    con = duckdb.connect()

    before = con.execute(
        "SELECT COUNT(*) FROM read_parquet($1)",
        [str(before_parquet)],
    ).fetchone()[0]

    after = con.execute(
        "SELECT COUNT(*) FROM read_parquet($1)",
        [str(after_parquet)],
    ).fetchone()[0]

    con.close()

    diff = after - before
    same = diff == 0

    print(f"Rows before: {before:,}")
    print(f"Rows after:  {after:,}")
    print(f"Difference:  {diff:+,}")

    if strict and not same:
        raise ValueError(
            f"Row count mismatch: before={before:,}, after={after:,}"
        )

    return {
        "before_file": str(before_parquet),
        "after_file": str(after_parquet),
        "rows_before": before,
        "rows_after": after,
        "difference": diff,
        "same": same,
    }


def add_beacon_metadata_to_latest_parquet(
    aggregations_dir: str | Path = "data/aggregations",
    metadata_json: str | Path = "data/beacons/beacon_downloads_metadata.json",
    output_suffix: str = "_addedmeta",
) -> Path:
    aggregations_dir = Path(aggregations_dir)
    metadata_json = Path(metadata_json)

    parquets = list(aggregations_dir.glob("*.parquet"))
    if not parquets:
        raise FileNotFoundError(f"No parquet files found in {aggregations_dir}")

    parquets = [
        p for p in aggregations_dir.glob("*.parquet")
        if not p.stem.endswith("_addedmeta")
    ]

    in_parquet = max(parquets, key=lambda p: p.stat().st_mtime)


    out_parquet = in_parquet.with_name(f"{in_parquet.stem}{output_suffix}{in_parquet.suffix}")

    print(f"Input parquet : {in_parquet}")
    print(f"Output parquet: {out_parquet}")
    print(f"Metadata JSON : {metadata_json}")

    con = duckdb.connect()

    # Optional: quick sanity check that JSON can be read
    con.execute(
        "SELECT path, url, download_time_utc FROM read_json_auto($1, format='auto') LIMIT 1",
        [str(metadata_json)],
    )

    sql = r"""
    COPY (
      WITH meta AS (
        SELECT
          path,
          url AS beacon_uri,
          download_time_utc AS beacon_harvest_timestamp
        FROM read_json_auto($1, format='auto')
      ),
      agg AS (
        SELECT
          *,
          replace(source_file, '\/', '/') AS source_file_norm
        FROM read_parquet($2)
      )
      SELECT
        agg.* EXCLUDE (source_file_norm),
        meta.beacon_uri,
        meta.beacon_harvest_timestamp
      FROM agg
      LEFT JOIN meta
        ON agg.source_file_norm = meta.path
    ) TO $3 (FORMAT PARQUET);
    """

    con.execute(sql, [str(metadata_json), str(in_parquet), str(out_parquet)])
    con.close()

    print("✔ Parquet written successfully.")
    return out_parquet


def analyze_parquet(parquet_path: str | Path, show_columns: bool = True) -> dict:
    """
    Analyze a parquet file using DuckDB (no pandas):
      - row count
      - column count
      - column names + types
      - count of "full-row duplicates" (rows identical across all columns)

    Returns a dict with results.
    """
    parquet_path = Path(parquet_path)

    if not parquet_path.exists():
        raise FileNotFoundError(parquet_path)

    con = duckdb.connect()

    # Column names + types (cheap; reads schema/metadata)
    col_info = con.execute(
        "DESCRIBE SELECT * FROM read_parquet($1)",
        [str(parquet_path)],
    ).fetchall()
    # DESCRIBE returns rows like: (column_name, column_type, null, null, null, null)
    columns = [(r[0], r[1]) for r in col_info]
    col_names = [c[0] for c in columns]
    col_count = len(columns)

    # Row count
    row_count = con.execute(
        "SELECT COUNT(*) FROM read_parquet($1)",
        [str(parquet_path)],
    ).fetchone()[0]

    # Full-row duplicate count:
    # duplicates = total_rows - number_of_distinct_rows_over_all_columns
    # Using DISTINCT on all columns is exactly what we want here.
    distinct_rows = con.execute(
        "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM read_parquet($1))",
        [str(parquet_path)],
    ).fetchone()[0]
    full_row_duplicates = row_count - distinct_rows

    con.close()

    result = {
        "file": str(parquet_path),
        "row_count": row_count,
        "column_count": col_count,
        "columns": columns,  # list of (name, type)
        "full_row_duplicates": full_row_duplicates,
        "distinct_rows": distinct_rows,
    }

    # Pretty output
    print(f"\nParquet: {parquet_path}")
    print(f"Rows:    {row_count:,}")
    print(f"Cols:    {col_count:,}")
    print(f"Distinct rows (all cols): {distinct_rows:,}")
    print(f"Full-row duplicates:      {full_row_duplicates:,}")

    if show_columns:
        print("\nColumns:")
        for name, typ in columns:
            print(f"  - {name}: {typ}")

    return result

def merge_parquets(
    parquet_files: Sequence[str],
    output_parquet: str,
    *,
    batch_size: int = 262_144,           # rows per batch (tune up/down)
    compression: str = "zstd",           # or "snappy", "gzip", None
    use_dictionary: bool = True,
) -> str:
    """
    Merge multiple Parquet files (same column names/structure) into a single Parquet file
    without holding all data in RAM.

    Parameters
    ----------
    parquet_files : list[str]
        Input parquet paths, in the order they should be appended.
    output_parquet : str
        Output parquet path to write.
    batch_size : int
        Rows to stream per batch from each source file.
    compression : str
        Parquet compression codec: "zstd", "snappy", "gzip", None, etc.
    use_dictionary : bool
        Whether to dictionary-encode eligible columns to reduce size.

    Returns
    -------
    str
        The output_parquet path.
    """
    if not parquet_files:
        raise ValueError("parquet_files is empty")

    # Use schema from the first file as the "target" schema
    first_pf = pq.ParquetFile(parquet_files[0])
    target_schema = first_pf.schema_arrow

    # make sure the merged folder exists && path is handled correctly
    output_parquet_path = Path(output_parquet)
    output_parquet_path.parent.mkdir(parents=True, exist_ok=True)
    

    writer: Optional[pq.ParquetWriter] = None

    try:
        writer = pq.ParquetWriter(
            str(output_parquet_path),
            target_schema,
            compression=compression,
            use_dictionary=use_dictionary,
        )

        for path in parquet_files:
            pf = pq.ParquetFile(path)

            # Stream record batches from each file
            for batch in pf.iter_batches(batch_size=batch_size):
                # Ensure batch matches the output schema (handles minor type drift)
                tbl = pa.Table.from_batches([batch])

                if tbl.schema != target_schema:
                    # Reorder / cast columns to match the first file's schema
                    tbl = tbl.select(target_schema.names).cast(target_schema, safe=False)

                writer.write_table(tbl)

    finally:
        if writer is not None:
            writer.close()

    return output_parquet


# Example usage:
# out = merge_parquets(
#     ["a.parquet", "b.parquet", "c.parquet", "d.parquet", "e.parquet"],
#     "merged.parquet",
#     batch_size=100_000,
# )
# print("Wrote:", out)



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



def look_into_parquet(filename):
    
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
