from __future__ import annotations

import argparse
from pathlib import Path

from beacons import EXPECTED_COLUMNS_DEFAULT
from layout import build_run_layout, default_run_id
from pipeline import (
    PipelineConfig,
    add_metadata_step,
    aggregate_step,
    download_step,
    match_step,
    merge_step,
    resolve_step,
    run_pipeline,
    sample_step,
    split_step,
)


def _parse_columns(value: str) -> list[str]:
    return [c.strip() for c in value.split(",") if c.strip()]


def _default_columns() -> list[str]:
    return list(EXPECTED_COLUMNS_DEFAULT)


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Base data directory (default: data)",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Run identifier (default: timestamp)",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="BEACONaggregator pipeline (RAM-friendly).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    pipeline = sub.add_parser("pipeline", help="Run full pipeline")
    _add_common_args(pipeline)
    pipeline.add_argument(
        "--columns",
        default=None,
        help="Comma-separated columns for beacon parquet schema",
    )
    pipeline.add_argument(
        "--no-latest-merged",
        action="store_true",
        help="Skip writing merged latest parquet",
    )

    download = sub.add_parser("download", help="Download beacon files")
    _add_common_args(download)

    aggregate = sub.add_parser("aggregate", help="Aggregate beacons to parquet")
    _add_common_args(aggregate)
    aggregate.add_argument("--columns", default=None, help="Comma-separated columns")

    add_meta = sub.add_parser("add-metadata", help="Add download metadata to parquet")
    _add_common_args(add_meta)
    add_meta.add_argument(
        "--in-parquet",
        default=None,
        help="Input parquet path (default: run aggregation parquet)",
    )

    split = sub.add_parser("split", help="Split aggregation parquet into variants")
    _add_common_args(split)
    split.add_argument(
        "--in-parquet",
        default=None,
        help="Input parquet path (default: run aggregation with metadata)",
    )

    resolve = sub.add_parser("resolve", help="Resolve target URLs for split parquets")
    resolve.add_argument(
        "parquets",
        nargs="+",
        help="Split parquet paths to resolve",
    )

    sample = sub.add_parser("sample", help="Create JSON samples from parquets")
    sample.add_argument("parquets", nargs="+", help="Parquet paths to sample")
    sample.add_argument(
        "--out-dir",
        required=True,
        help="Directory for sample JSON files",
    )

    match = sub.add_parser("match", help="Match beaconlist entries to parquets")
    match.add_argument("parquets", nargs="+", help="Parquet paths to match")
    match.add_argument(
        "--out-dir",
        required=True,
        help="Directory for match JSON output",
    )
    match.add_argument(
        "--out-name",
        default=None,
        help="Filename for match JSON output",
    )

    merge = sub.add_parser("merge", help="Merge parquets")
    merge.add_argument("parquets", nargs="+", help="Parquet paths to merge")
    merge.add_argument("--out-parquet", required=True, help="Output merged parquet")
    merge.add_argument(
        "--latest-parquet",
        default=None,
        help="Optional latest parquet path to overwrite",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "pipeline":
        run_id = args.run_id or default_run_id()
        columns = _parse_columns(args.columns) if args.columns else _default_columns()
        config = PipelineConfig(
            data_dir=Path(args.data_dir),
            run_id=run_id,
            expected_columns=columns,
            write_latest_merged=not args.no_latest_merged,
        )
        run_pipeline(config)
        return

    if args.command == "download":
        download_step(args.data_dir, args.run_id)
        return

    if args.command == "aggregate":
        if not args.run_id:
            parser.error("aggregate requires --run-id")
        columns = _parse_columns(args.columns) if args.columns else _default_columns()
        aggregate_step(args.data_dir, args.run_id, expected_columns=columns)
        return

    if args.command == "add-metadata":
        if not args.run_id:
            parser.error("add-metadata requires --run-id")
        add_metadata_step(args.data_dir, args.run_id, in_parquet=args.in_parquet)
        return

    if args.command == "split":
        if not args.run_id:
            parser.error("split requires --run-id")
        split_step(args.data_dir, args.run_id, in_parquet=args.in_parquet)
        return

    if args.command == "resolve":
        resolve_step(args.parquets)
        return

    if args.command == "sample":
        sample_step(args.parquets, out_dir=args.out_dir)
        return

    if args.command == "match":
        match_step(args.parquets, out_dir=args.out_dir, out_name=args.out_name)
        return

    if args.command == "merge":
        merge_step(args.parquets, args.out_parquet, latest_path=args.latest_parquet)
        return


if __name__ == "__main__":
    main()
