from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


def default_run_id() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


@dataclass(frozen=True)
class RunLayout:
    data_dir: Path
    run_id: str
    beacons_dir: Path
    aggregations_dir: Path
    split_dir: Path
    split_samples_dir: Path
    merged_dir: Path
    beaconlist_dir: Path
    download_metadata_path: Path
    aggregation_parquet: Path
    aggregation_with_metadata_parquet: Path
    merged_parquet: Path
    merged_latest_parquet: Path
    beaconlist_matches_json: Path


def build_run_layout(data_dir: str | Path, run_id: str) -> RunLayout:
    data_dir = Path(data_dir)
    beacons_dir = data_dir / "beacons" / run_id
    aggregations_dir = data_dir / "aggregations" / run_id
    split_dir = data_dir / "split_aggregations"
    split_samples_dir = split_dir / "samples"
    merged_dir = data_dir / "merged"
    beaconlist_dir = data_dir / "beaconlist"

    aggregation_parquet = aggregations_dir / f"beacons_{run_id}.parquet"
    aggregation_with_metadata_parquet = aggregations_dir / f"beacons_{run_id}_addedmeta.parquet"
    merged_parquet = merged_dir / f"beacons_merged_{run_id}.parquet"
    merged_latest_parquet = merged_dir / "beacons_merged_latest.parquet"
    beaconlist_matches_json = beaconlist_dir / f"beacon_parquet_contains_matches_{run_id}.json"
    download_metadata_path = beacons_dir / "beacon_downloads_metadata.json"

    return RunLayout(
        data_dir=data_dir,
        run_id=run_id,
        beacons_dir=beacons_dir,
        aggregations_dir=aggregations_dir,
        split_dir=split_dir,
        split_samples_dir=split_samples_dir,
        merged_dir=merged_dir,
        beaconlist_dir=beaconlist_dir,
        download_metadata_path=download_metadata_path,
        aggregation_parquet=aggregation_parquet,
        aggregation_with_metadata_parquet=aggregation_with_metadata_parquet,
        merged_parquet=merged_parquet,
        merged_latest_parquet=merged_latest_parquet,
        beaconlist_matches_json=beaconlist_matches_json,
    )
