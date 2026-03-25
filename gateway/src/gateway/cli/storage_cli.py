"""Offline Layer 3 preprocessing and export commands."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Sequence

from gateway.preprocess.export import export_training_dataset, preprocess_date_range
from gateway.storage.paths import build_storage_paths


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse storage-layer subcommands."""
    parser = argparse.ArgumentParser(description="Warehouse Spice Risk Layer 3 storage tools")
    subparsers = parser.add_subparsers(dest="command", required=True)

    preprocess_parser = subparsers.add_parser("preprocess", help="Create processed daily datasets from raw CSV files.")
    pod_group = preprocess_parser.add_mutually_exclusive_group(required=True)
    pod_group.add_argument("--pod", action="append", help="Pod id to preprocess. Repeat for multiple pods.")
    pod_group.add_argument("--all", action="store_true", help="Preprocess all pods found under data/raw/pods.")
    date_group = preprocess_parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument("--date", help="Single UTC day to preprocess (YYYY-MM-DD).")
    date_group.add_argument("--from", dest="date_from", help="Start UTC day for a preprocessing range.")
    preprocess_parser.add_argument("--to", dest="date_to", help="End UTC day for a preprocessing range.")
    preprocess_parser.add_argument("--interval", type=int, default=60, help="Resample interval in seconds.")
    preprocess_parser.add_argument("--interpolate", action="store_true", help="Linearly interpolate small gaps.")
    preprocess_parser.add_argument("--max-gap-minutes", type=int, default=5, help="Maximum gap size to interpolate.")
    preprocess_parser.add_argument("--temp-min-c", type=float, default=-20.0, help="Minimum valid temperature.")
    preprocess_parser.add_argument("--temp-max-c", type=float, default=80.0, help="Maximum valid temperature.")

    export_parser = subparsers.add_parser("export-training", help="Concatenate processed CSV files for later ML work.")
    export_parser.add_argument("--from", dest="date_from", required=True, help="Start UTC day for export.")
    export_parser.add_argument("--to", dest="date_to", required=True, help="End UTC day for export.")
    export_parser.add_argument(
        "--out",
        default=None,
        help="Output CSV path. Defaults to data/exports/training_dataset.csv.",
    )

    args = parser.parse_args(argv)

    if args.command == "preprocess":
        if args.date is not None:
            if args.date_from is not None or args.date_to is not None:
                parser.error("preprocess does not allow --from/--to together with --date.")
        else:
            if args.date_from is None or args.date_to is None:
                parser.error("preprocess requires both --from and --to when not using --date.")
        if args.interval <= 0:
            parser.error("--interval must be greater than 0.")
        if args.max_gap_minutes < 0:
            parser.error("--max-gap-minutes must be 0 or greater.")
        if args.temp_min_c >= args.temp_max_c:
            parser.error("--temp-min-c must be lower than --temp-max-c.")

    return args


def cli(argv: Sequence[str] | None = None) -> int:
    """Run the requested offline storage command."""
    args = parse_args(argv)
    storage_paths = build_storage_paths()

    if args.command == "preprocess":
        if args.date is not None:
            date_from = date_to = date.fromisoformat(args.date)
        else:
            date_from = date.fromisoformat(args.date_from)
            date_to = date.fromisoformat(args.date_to)
        outputs = preprocess_date_range(
            data_root=storage_paths.root,
            pod_ids=None if args.all else args.pod,
            date_from=date_from,
            date_to=date_to,
            interval_s=args.interval,
            interpolate=args.interpolate,
            max_gap_minutes=args.max_gap_minutes,
            temp_min_c=args.temp_min_c,
            temp_max_c=args.temp_max_c,
        )
        for path in outputs:
            print(path)
        return 0

    out_path = Path(args.out) if args.out is not None else storage_paths.training_export_path()
    result = export_training_dataset(
        data_root=storage_paths.root,
        date_from=date.fromisoformat(args.date_from),
        date_to=date.fromisoformat(args.date_to),
        out_path=out_path,
    )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(cli())
