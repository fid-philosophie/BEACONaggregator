# main.py

#from __future__ import annotations

import gc
import json
import re
import duckdb
import pandas as pd
import pyarrow.parquet as pq
import argparse
import sys

from pathlib import Path
from datetime import datetime
from typing import Optional, Sequence, Any, Dict, Union, List
from collections import defaultdict

from handle_parquets import inspect_parquet_with_duckdb, qident, look_into_parquet, merge_parquets, add_beacon_metadata_to_latest_parquet, analyze_parquet, compare_parquet_row_counts, count_parquet_rows
from helpers import make_safe_prefix, analyze_json_path_uniqueness
from beacons import download_beacon_file, parse_beacon_file, load_beacon_list, collect_beacons_to_dataframe, iter_beacon_dataframes, EXPECTED_COLUMNS, download_from_beaconlist

# BEACONlist default folder (if used with --pick)
BEACONLIST_DIR = Path("data/beaconlist")

#BEACONLIST_DIR = Path("./beaconlists")
DOWNLOADED_BEACONS_BASE_DIR = Path("data/beacons")

def add_contains_matches_and_dump_json(
    df: pd.DataFrame,
    parquet_paths: list[str | Path],
    out_dir: str | Path = "data/beaconlist_matches",
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


def create_resolved_parquets(parquet_paths, beacons_dir):
    #beacons_dir = Path("data/beacons")
    with open(beacons_dir / "beacon_downloads_metadata.json", "r", encoding="utf-8") as f:
        beacon_metadata = json.load(f)


    for path in parquet_paths:
        actualpath = Path(path)
        name = actualpath.name

        resolved_expr = None      # reset each loop
        target_id_expr = None
        authority_id_expr = None
        id_col = None




        # Decide which column provides the ID/value + build target_id/authority_id
        if "target-notnull" in name and "col1-2-3" in name:
            #id_col = "col3"
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
            # last part after last "/"
            target_id_expr = "regexp_extract(col3, '([^/]+)$', 1)"
            authority_id_expr = "col1"

        elif "target-null" in name and "col1-2" in name:
            resolved_expr = "col2"
            # last part after last "/"
            target_id_expr = "regexp_extract(col2, '([^/]+)$', 1)"
            authority_id_expr = "col1"

        else:
            print(f"Skipping {name}: no matching rule for filename")
            continue

        # Build resolved_expr for target-notnull cases
        if "target-notnull" in name:
            # resolved_expr = f"""
            #     CASE
            #         WHEN TARGET LIKE '%{{ID}}%'
            #             THEN REPLACE(TARGET, '{{ID}}', CAST({id_col} AS VARCHAR))
            #         ELSE TARGET || ' ' || CAST({id_col} AS VARCHAR)
            #     END
            # """
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
                    {resolved_expr} AS target_uri,
                    {target_id_expr} AS target_id,
                    {authority_id_expr} AS authority_id,
                    beacon_uri,
                    beacon_harvest_timestamp
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


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run aggregation pipeline"
    )

    group_beaconlist = parser.add_mutually_exclusive_group()
    group_beaconlist.title = "Beaconlist source"

    group_beaconlist.add_argument(
        "-b",
        "--beaconlist",
        type=str,
        help="Path or URL to beacon list (optional)"
    )

    group_beaconlist.add_argument(
        "-p",
        "--pick",
        action="store_true",
        help="Interactively pick a beacon list from BEACONLIST_DIR",
    )

    group_beacons = parser.add_mutually_exclusive_group()
    group_beacons.title = "Beacons directory source"

    group_beacons.add_argument(
        "-r",
        "--reuse-beacons",
        action="store_true",
        help="Pick an existing beacons directory",
    )

    group_beacons.add_argument(
        "--beacons-dir",
        type=str,
        help="Use a specific existing beacons directory",
    )

    return parser.parse_args()


def pick_beaconlist() -> str:
    files = sorted([f for f in BEACONLIST_DIR.iterdir() if f.is_file()])

    if not files:
        print(f"No files found in {BEACONLIST_DIR}")
        sys.exit(0)  # graceful exit

    print("Select a beacon list:\n")
    for i, f in enumerate(files, start=1):
        print(f"{i}: {f.name}")

    while True:
        choice = input("\nEnter number: ").strip()

        if not choice.isdigit():
            print("Please enter a valid number.")
            continue

        idx = int(choice) - 1
        if 0 <= idx < len(files):
            return str(files[idx])

        print("Choice out of range.")


def make_new_beacons_dir_path() -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    return Path(f"data/beacons_{timestamp}")


def pick_existing_beacons_dir() -> Path:
    """
    Pick an existing directory from data/beacons matching beacons_*.
    If none exist, fall back to Path('data/beacons_{timestamp}').
    """
    if not DOWNLOADED_BEACONS_BASE_DIR.exists():
        return make_new_beacons_dir_path()

    dirs = sorted(
        [
            p for p in DOWNLOADED_BEACONS_BASE_DIR.iterdir()
            if p.is_dir() and p.name.startswith("beacons_")
        ],
        reverse=True,  # newest-looking names first
    )

    if not dirs:
        return make_new_beacons_dir_path()

    print("Select an existing downloaded beacons directory:\n")
    for i, d in enumerate(dirs, start=1):
        print(f"{i}: {d.name}")

    print("0: Create/use a new timestamped directory")

    while True:
        choice = input("\nEnter number: ").strip()

        if not choice.isdigit():
            print("Please enter a valid number.")
            continue

        idx = int(choice)

        if idx == 0:
            #return make_new_beacons_dir_path()
            return None

        if 1 <= idx <= len(dirs):
            return dirs[idx - 1]

        print("Choice out of range.")


def main_pipeline(
        beaconlist_location: str | None = None,
        beacons_dir: Path | None = None,
        ) -> None:
    if beaconlist_location:
        print(f"Using beacon list from: {beaconlist_location}")
    else:
        print("Using default beacon list")

    if beaconlist_location:
        print(f"Using beacons_dir = {beacons_dir}")
    else:
        print("Using new beacons_dir & will start fresh downloads")
    
    data_dir = Path("data")
    #parquet_path = data_dir / "aggregations" / "beacons_20251212-1605.parquet"
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    parquet_path = (data_dir / "aggregations" / f"beacons_{timestamp}.parquet")
    merged_parquet_path = (data_dir / "merged" / f"beacons_merged_{timestamp}.parquet")


    # list of parquet files the aggregation will be split into (= BEACON variants)
    parquet_toresolveurls_paths = [
        "data/split_aggregations/target-null_col1-2.parquet",
        "data/split_aggregations/target-null_col1-2-3.parquet",
        "data/split_aggregations/target-notnull_col1-only.parquet",
        "data/split_aggregations/target-notnull_col1-2.parquet",
        "data/split_aggregations/target-notnull_col1-2-3.parquet",
    ]

    # list of parquet files with resolved uris (e.g. repalced "{ID}" placeholders)
    parquet_withresolvedurls_paths = [
        "data/split_aggregations/target-null_col1-2_resolved.parquet",
        "data/split_aggregations/target-null_col1-2-3_resolved.parquet",
        "data/split_aggregations/target-notnull_col1-only_resolved.parquet",
        "data/split_aggregations/target-notnull_col1-2_resolved.parquet",
        "data/split_aggregations/target-notnull_col1-2-3_resolved.parquet",
    ]


    parquet_dir = data_dir / "split_aggregations"

    if not(beacons_dir):
        beacons_dir = Path(f"data/beacons/beacons_{timestamp}")
        download_from_beaconlist(beaconlist_location, beacons_dir)
    #beacons_dir = Path(f"data/beacons/beacons_{timestamp}") # comment out, if see below
    
    #beacons_dir = Path(f"data/beacons/beacons_20251218-1754") # uncomment and set custom name => overwrite timestamped dir name, if needed

    # download each BEACON file into folder data/beacons/ (~270MB + takes some time)
    # cf. data/beacons/beacon_downloads_metadata.json
    #download_from_beaconlist(out_dir=beacons_dir)
    #download_from_beaconlist(beaconlist_location=beaconlist_location, out_dir=beacons_dir)
    
    #download_from_beaconlist(beaconlist_location, beacons_dir)

    # beacons in 1 parquet umwandeln:
    parquet_path = beacons_to_parquet(beacons_dir=beacons_dir)

    # parquet auswerten:
    #look_into_parquet(parquet_path)

    # add metadata:
    metadata_json = beacons_dir / "beacon_downloads_metadata.json"
    parquet_with_metadata_path = add_beacon_metadata_to_latest_parquet(metadata_json=metadata_json)
    print(parquet_with_metadata_path)

    # # split by target null/not null and col1-3:
    # split_parquet(parquet_path) 
    split_parquet(parquet_with_metadata_path) 

    # # # Point this to your directory with parquet files
    # # samples = sample_parquet_dir(parquet_dir)
    # # # `samples` is now a dict of DataFrames for quick exploration in Python:
    # # # e.g. see the sample for one file:
    # # samples["my_file.parquet"].head()

    # resolve urls for all types of beacons and create new parquets:
    create_resolved_parquets(parquet_toresolveurls_paths, beacons_dir)
    
    # make inspectable json files (with samples) from single parquets
    make_inspectable_jsons_from_parquets(parquet_withresolvedurls_paths)

    # get/load BEACONlist
    beaconlist_json = load_beacon_list(beaconlist_location)
    df_beaconlist = pd.DataFrame(beaconlist_json)

    # match BEACONlist with parquet splits (=> which BEACON uses which BEACON format variants)
    out_path = add_contains_matches_and_dump_json(df_beaconlist, parquet_withresolvedurls_paths)

    print(out_path)


    # merge parquets into 1:
    merge_parquets(parquet_withresolvedurls_paths, merged_parquet_path)
    
    

def main():

    args = parse_args()

    # --- beaconlist ---
    beaconlist_location = args.beaconlist
    if args.pick:
        beaconlist_location = pick_beaconlist()

    # --- beacons dir ---
    beacons_dir = None
    if args.beacons_dir:
        beacons_dir = Path(args.beacons_dir)
    elif args.reuse_beacons:  # or args.pick_beacons_dir
        beacons_dir = pick_existing_beacons_dir()

    main_pipeline(
        beaconlist_location=beaconlist_location,
        beacons_dir=beacons_dir,
    )



    #main_pipeline(beaconlist_location)

    #data_dir = Path("data")
    #analyze_parquet(data_dir / "merged" / "beacons_merged_20251216-1627.parquet")

    #analyze_json_path_uniqueness("data/beacons/beacon_downloads_metadata.json")

    # in_parquet = data_dir / "aggregations" / "beacons_20251215-2059.parquet"
    # out_parquet = data_dir / "aggregations" / "beacons_20251215-2059_addedmeta.parquet"
    # merged_parquet = data_dir / "merged" / "beacons_merged_20251216-1627.parquet"
    # compare_parquet_row_counts(in_parquet, out_parquet, strict=True)

    # print(count_parquet_rows(merged_parquet))

    # data_dir = Path("data")
    # parquet_path = data_dir / "aggregations" / "beacons_20251212-1605.parquet"
    # merged_parquet_path = data_dir / "merged" / f"beacons_merged_{datetime.now().strftime("%Y%m%d-%H%M")}.parquet"

    # # list of parquet files the aggregation will be split into (= BEACON variants)
    # parquet_toresolveurls_paths = [
    #     "data/split_aggregations/target-null_col1-2.parquet",
    #     "data/split_aggregations/target-null_col1-2-3.parquet",
    #     "data/split_aggregations/target-notnull_col1-only.parquet",
    #     "data/split_aggregations/target-notnull_col1-2.parquet",
    #     "data/split_aggregations/target-notnull_col1-2-3.parquet",
    # ]

    # # list of parquet files with resolved uris (e.g. repalced "{ID}" placeholders)
    # parquet_withresolvedurls_paths = [
    #     "data/split_aggregations/target-null_col1-2_resolved.parquet",
    #     "data/split_aggregations/target-null_col1-2-3_resolved.parquet",
    #     "data/split_aggregations/target-notnull_col1-only_resolved.parquet",
    #     "data/split_aggregations/target-notnull_col1-2_resolved.parquet",
    #     "data/split_aggregations/target-notnull_col1-2-3_resolved.parquet",
    # ]


    # parquet_dir = data_dir / "split_aggregations"

    # # download each BEACON file into folder data/beacons/ (~270MB + takes some time)
    # # cf. data/beacons/beacon_downloads_metadata.json
    # download_from_beaconlist()

    # # beacons in 1 parquet umwandeln:
    # parquet_path = beacons_to_parquet()

    # # parquet auswerten:
    # #look_into_parquet(parquet_path)

    # parquet_with_metadata_path = add_beacon_metadata_to_latest_parquet()
    # print(parquet_with_metadata_path)

    # # # split by target null/not null and col1-3:
    # # split_parquet(parquet_path) 
    # split_parquet(parquet_with_metadata_path) 

    # # # # Point this to your directory with parquet files
    # # # samples = sample_parquet_dir(parquet_dir)
    # # # # `samples` is now a dict of DataFrames for quick exploration in Python:
    # # # # e.g. see the sample for one file:
    # # # samples["my_file.parquet"].head()

    # # resolve urls for all types of beacons and create new parquets:
    # create_resolved_parquets(parquet_toresolveurls_paths)
    
    # # make inspectable json files (with samples) from single parquets
    # make_inspectable_jsons_from_parquets(parquet_withresolvedurls_paths)

    # # get/load BEACONlist
    # beaconlist_json = load_beacon_list()
    # df_beaconlist = pd.DataFrame(beaconlist_json)

    # # match BEACONlist with parquet splits (=> which BEACON uses which BEACON format variants)
    # out_path = add_contains_matches_and_dump_json(df_beaconlist, parquet_withresolvedurls_paths)

    # print(out_path)


    # # merge parquets into 1:
    # merge_parquets(parquet_withresolvedurls_paths, merged_parquet_path)


    # # Hagrid NDIF fields
    # # target_uri, normdata_uri, type, role, label, project, source_date, date_of_export
    # # wikidata/gnd?
    # # feed/beacon_url?
    # # ?
    # # target id ?
    # # authority id ? / source/reference/authority id?!




if __name__ == "__main__":
    main()
