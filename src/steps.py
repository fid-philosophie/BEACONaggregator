from __future__ import annotations

import gc
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from beacons import EXPECTED_COLUMNS_DEFAULT, iter_beacon_dataframes, load_beacon_list
from handle_parquets import inspect_parquet_with_duckdb, merge_parquets
from helpers import make_safe_prefix


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

    if "beacon_url" not in df.columns:
        raise KeyError("df must contain column 'beacon_url'")

    df = df.copy()

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

    if not prefixes:
        df["matched_parquets"] = [[] for _ in range(len(df))]
        return _dump_df_json(df, out_dir=out_dir, out_name=out_name)

    compiled: list[re.Pattern] = []
    for i in range(0, len(prefixes), regex_chunk_size):
        chunk = prefixes[i : i + regex_chunk_size]
        pat = "(" + "|".join(re.escape(p) for p in chunk) + ")"
        compiled.append(re.compile(pat))

    found_in: dict[str, list[str]] = defaultdict(list)

    for p in map(Path, parquet_paths):
        p = p.resolve()
        if not p.exists():
            print(f"Skipping missing parquet: {p}")
            continue

        print(f"Scanning {p} ...")
        pf = pq.ParquetFile(p)

        seen_this_file: set[str] = set()

        for batch in pf.iter_batches(columns=["source_file"], batch_size=batch_size):
            values = batch.column(0).to_pylist()

            for s in values:
                if s is None:
                    continue

                for rx in compiled:
                    m = rx.search(s)
                    if not m:
                        continue

                    for mm in rx.finditer(s):
                        pref = mm.group(0)
                        if pref not in seen_this_file:
                            found_in[pref].append(str(p))
                            seen_this_file.add(pref)

    df["matched_parquets"] = df["safe_prefix"].map(
        lambda v: found_in.get(str(v), []) if pd.notna(v) else []
    )

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


def beacons_to_parquet(
    beacons_dir: str | Path,
    out_dir: str | Path,
    out_path: str | Path | None = None,
    skip_suffixes: tuple[str, ...] = (".json",),
    expected_columns: list[str] | None = None,
) -> Path:
    """
    Stream BEACON files to ONE parquet without holding everything in RAM.
    """
    beacons_dir = Path(beacons_dir)
    out_dir = Path(out_dir)
    beacons_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    if out_path is None:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        parquet_path = out_dir / f"beacons_{timestamp}.parquet"
    else:
        parquet_path = Path(out_path)
        parquet_path.parent.mkdir(parents=True, exist_ok=True)

    if expected_columns is None:
        expected_columns = list(EXPECTED_COLUMNS_DEFAULT)

    arrow_schema = pa.schema([(c, pa.string()) for c in expected_columns])

    writer: pq.ParquetWriter | None = None
    total_rows = 0

    try:
        for df in iter_beacon_dataframes(
            beacons_dir,
            skip_suffixes=skip_suffixes,
            expected_columns=expected_columns,
        ):
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


def split_parquet(filename: str | Path, out_dir: str | Path) -> list[Path]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    configs = [
        ("target-null_col1-only", "TARGET IS NULL", "col1 IS NOT NULL AND col2 IS NULL     AND col3 IS NULL"),
        ("target-null_col1-2", "TARGET IS NULL", "col1 IS NOT NULL AND col2 IS NOT NULL AND col3 IS NULL"),
        ("target-null_col1-2-3", "TARGET IS NULL", "col1 IS NOT NULL AND col2 IS NOT NULL AND col3 IS NOT NULL"),
        ("target-notnull_col1-only", "TARGET IS NOT NULL", "col1 IS NOT NULL AND col2 IS NULL     AND col3 IS NULL"),
        ("target-notnull_col1-2", "TARGET IS NOT NULL", "col1 IS NOT NULL AND col2 IS NOT NULL AND col3 IS NULL"),
        ("target-notnull_col1-2-3", "TARGET IS NOT NULL", "col1 IS NOT NULL AND col2 IS NOT NULL AND col3 IS NOT NULL"),
    ]

    out_paths: list[Path] = []

    for suffix, target_cond, col_cond in configs:
        where_clause = f"WHERE {target_cond} AND {col_cond}"

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
            continue

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
        out_paths.append(parquet_path)

    return out_paths


def sample_parquet_dir(
    dir_path: str | Path,
    n_rows: int = 100,
    pattern: str = "*.parquet",
    random_state: int = 42,
) -> dict[str, pd.DataFrame]:
    dir_path = Path(dir_path)
    if not dir_path.is_dir():
        raise NotADirectoryError(f"{dir_path} is not a directory")

    samples = {}

    for parquet_path in dir_path.glob(pattern):
        print(f"Processing {parquet_path.name}...")
        df = pd.read_parquet(parquet_path)

        if len(df) > n_rows:
            df_sample = df.sample(n=n_rows, random_state=random_state)
        else:
            df_sample = df

        json_path = parquet_path.with_suffix(".json")
        df_sample.to_json(json_path, orient="records", lines=False, indent=2)

        samples[parquet_path.name] = df_sample

    return samples


def create_resolved_parquets(parquet_paths: Iterable[str | Path]) -> list[Path]:
    out_paths: list[Path] = []

    for path in parquet_paths:
        actualpath = Path(path)
        name = actualpath.name

        resolved_expr = None
        target_id_expr = None
        authority_id_expr = None
        id_col = None

        if "target-notnull" in name and "col1-2-3" in name:
            id_col = "col1"
            target_id_expr = "col3"
            authority_id_expr = "col1"

        elif "target-notnull" in name and "col1-2" in name:
            id_col = "col1"
            target_id_expr = "col1"
            authority_id_expr = "col1"

        elif "target-notnull" in name and "col1-only" in name:
            id_col = "col1"
            target_id_expr = "col1"
            authority_id_expr = "col1"

        elif "target-null" in name and "col1-2-3" in name:
            resolved_expr = "col3"
            target_id_expr = "regexp_extract(col3, '([^/]+)$', 1)"
            authority_id_expr = "col1"

        elif "target-null" in name and "col1-2" in name:
            resolved_expr = "col2"
            target_id_expr = "regexp_extract(col2, '([^/]+)$', 1)"
            authority_id_expr = "col1"

        else:
            print(f"Skipping {name}: no matching rule for filename")
            continue

        if "target-notnull" in name:
            resolved_expr = f"""
                CASE
                    WHEN TARGET LIKE '%{{ID}}%'
                        THEN REPLACE(TARGET, '{{ID}}', CAST({id_col} AS VARCHAR))
                    ELSE
                        regexp_replace(TARGET, '/?$', '/') || CAST({id_col} AS VARCHAR)
                END
            """

        out_path = actualpath.with_stem(actualpath.stem + "_resolved")
        print(f"Creating {out_path.name} (rule from {name})")

        duckdb.sql(
            f"""
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
                    {resolved_expr} AS target_uri,
                    {target_id_expr} AS target_id,
                    {authority_id_expr} AS authority_id,
                    beacon_uri,
                    beacon_harvest_timestamp
                FROM '{actualpath.as_posix()}'
            ) TO '{out_path.as_posix()}'
            (FORMAT 'parquet');
            """
        )

        print(f"  -> wrote {out_path}")
        out_paths.append(out_path)

    return out_paths


def dump_df_samples_to_json(
    df: pd.DataFrame,
    parquet_file: str | Path,
    out_dir: str | Path | None = None,
) -> Path:
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

    df.to_json(
        json_path,
        orient="records",
        indent=2,
        force_ascii=False,
    )

    return json_path


def make_inspectable_jsons_from_parquets(
    parquet_paths: Iterable[str | Path],
    out_dir: str | Path | None = None,
) -> list[Path]:
    out_paths: list[Path] = []

    for parquet_path in parquet_paths:
        info = inspect_parquet_with_duckdb(
            Path(parquet_path),
            preview_rows=10,
            random_sample_rows=100,
            sample_seed=42,
            compute_row_count=True,
            stats_mode="sample",
        )

        df_sample = info.random_sample_df
        json_path = dump_df_samples_to_json(
            df_sample,
            parquet_path,
            out_dir=out_dir,
        )
        print(f"Wrote JSON samples: {json_path}")
        out_paths.append(json_path)

    return out_paths


def match_beaconlist_and_dump_json(
    parquet_paths: list[str | Path],
    out_dir: str | Path,
    out_name: str | None = None,
) -> Path:
    beaconlist_json = load_beacon_list()
    df_beaconlist = pd.DataFrame(beaconlist_json)
    return add_contains_matches_and_dump_json(
        df_beaconlist,
        parquet_paths,
        out_dir=out_dir,
        out_name=out_name,
    )


def merge_parquet_files(
    parquet_paths: list[str | Path],
    out_path: str | Path,
) -> Path:
    if not parquet_paths:
        raise ValueError("No parquet files provided to merge.")
    merge_parquets([str(p) for p in parquet_paths], str(out_path))
    return Path(out_path)
