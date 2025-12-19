from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from beacons import EXPECTED_COLUMNS_DEFAULT, download_from_beaconlist
from handle_parquets import add_beacon_metadata_to_parquet
from layout import RunLayout, build_run_layout, default_run_id
from steps import (
    beacons_to_parquet,
    create_resolved_parquets,
    make_inspectable_jsons_from_parquets,
    match_beaconlist_and_dump_json,
    merge_parquet_files,
    split_parquet,
)


@dataclass
class PipelineConfig:
    data_dir: Path = Path("data")
    run_id: str = field(default_factory=default_run_id)
    expected_columns: list[str] = field(default_factory=lambda: list(EXPECTED_COLUMNS_DEFAULT))
    write_latest_merged: bool = True
    split_dir: Path | None = None
    split_samples_dir: Path | None = None


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def run_pipeline(config: PipelineConfig) -> RunLayout:
    layout = build_run_layout(config.data_dir, config.run_id)
    print(f"Run ID: {layout.run_id}")

    _ensure_dir(layout.beacons_dir)
    _ensure_dir(layout.aggregations_dir)
    _ensure_dir(layout.split_dir)
    _ensure_dir(layout.split_samples_dir)
    _ensure_dir(layout.merged_dir)
    _ensure_dir(layout.beaconlist_dir)

    download_from_beaconlist(out_dir=layout.beacons_dir)

    raw_parquet = beacons_to_parquet(
        beacons_dir=layout.beacons_dir,
        out_dir=layout.aggregations_dir,
        out_path=layout.aggregation_parquet,
        expected_columns=config.expected_columns,
    )

    parquet_with_meta = add_beacon_metadata_to_parquet(
        in_parquet=raw_parquet,
        metadata_json=layout.download_metadata_path,
        out_parquet=layout.aggregation_with_metadata_parquet,
    )

    split_paths = split_parquet(parquet_with_meta, out_dir=layout.split_dir)
    if not split_paths:
        raise ValueError("Split step produced no parquet outputs.")
    resolved_paths = create_resolved_parquets(split_paths)
    if not resolved_paths:
        raise ValueError("Resolve step produced no parquet outputs.")

    make_inspectable_jsons_from_parquets(
        resolved_paths,
        out_dir=layout.split_samples_dir,
    )

    match_beaconlist_and_dump_json(
        resolved_paths,
        out_dir=layout.beaconlist_dir,
        out_name=layout.beaconlist_matches_json.name,
    )

    merged_path = merge_parquet_files(resolved_paths, layout.merged_parquet)
    if config.write_latest_merged:
        shutil.copy2(merged_path, layout.merged_latest_parquet)

    return layout


def download_step(
    data_dir: str | Path,
    run_id: str | None = None,
) -> RunLayout:
    if run_id is None:
        run_id = default_run_id()
    layout = build_run_layout(data_dir, run_id)
    print(f"Run ID: {layout.run_id}")
    _ensure_dir(layout.beacons_dir)
    download_from_beaconlist(out_dir=layout.beacons_dir)
    return layout


def aggregate_step(
    data_dir: str | Path,
    run_id: str,
    expected_columns: list[str] | None = None,
) -> Path:
    layout = build_run_layout(data_dir, run_id)
    _ensure_dir(layout.aggregations_dir)
    if expected_columns is None:
        expected_columns = list(EXPECTED_COLUMNS_DEFAULT)
    return beacons_to_parquet(
        beacons_dir=layout.beacons_dir,
        out_dir=layout.aggregations_dir,
        out_path=layout.aggregation_parquet,
        expected_columns=expected_columns,
    )


def add_metadata_step(
    data_dir: str | Path,
    run_id: str,
    in_parquet: str | Path | None = None,
) -> Path:
    layout = build_run_layout(data_dir, run_id)
    if in_parquet is None:
        in_parquet = layout.aggregation_parquet
    return add_beacon_metadata_to_parquet(
        in_parquet=in_parquet,
        metadata_json=layout.download_metadata_path,
        out_parquet=layout.aggregation_with_metadata_parquet,
    )


def split_step(
    data_dir: str | Path,
    run_id: str,
    in_parquet: str | Path | None = None,
) -> list[Path]:
    layout = build_run_layout(data_dir, run_id)
    if in_parquet is None:
        in_parquet = layout.aggregation_with_metadata_parquet
    _ensure_dir(layout.split_dir)
    return split_parquet(in_parquet, out_dir=layout.split_dir)


def resolve_step(
    parquet_paths: Iterable[str | Path],
) -> list[Path]:
    return create_resolved_parquets(parquet_paths)


def sample_step(
    parquet_paths: Iterable[str | Path],
    out_dir: str | Path,
) -> list[Path]:
    return make_inspectable_jsons_from_parquets(parquet_paths, out_dir=out_dir)


def match_step(
    parquet_paths: list[str | Path],
    out_dir: str | Path,
    out_name: str | None = None,
) -> Path:
    return match_beaconlist_and_dump_json(
        parquet_paths,
        out_dir=out_dir,
        out_name=out_name,
    )


def merge_step(
    parquet_paths: list[str | Path],
    out_parquet: str | Path,
    latest_path: str | Path | None = None,
) -> Path:
    merged_path = merge_parquet_files(parquet_paths, out_parquet)
    if latest_path is not None:
        shutil.copy2(merged_path, latest_path)
    return merged_path
