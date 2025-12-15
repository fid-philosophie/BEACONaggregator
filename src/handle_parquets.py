import pyarrow.parquet as pq
from pathlib import Path
from typing import Optional, Sequence, Any, Dict, Union, List
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
