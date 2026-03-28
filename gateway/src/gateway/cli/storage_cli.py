"""Offline Layer 3 preprocessing and export commands."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Sequence

from gateway.preprocess.export import export_training_dataset, preprocess_date_range
from gateway.storage.export_csv import export_all_pods_csv, export_pod_csv
from gateway.storage.import_csv import import_csv_history
from gateway.storage.paths import build_storage_paths
from gateway.storage.sqlite_db import init_db, resolve_db_path
from gateway.storage.sqlite_reader import latest_sample


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

    init_db_parser = subparsers.add_parser("init-db", help="Create the telemetry SQLite database and schema.")
    init_db_parser.add_argument(
        "--db-path",
        default="data/db/telemetry.sqlite",
        help="SQLite database path. Defaults to data/db/telemetry.sqlite.",
    )

    latest_parser = subparsers.add_parser("latest", help="Print the latest sample for one pod from SQLite.")
    latest_parser.add_argument("--pod", required=True, help="Pod id to query.")
    latest_parser.add_argument(
        "--db-path",
        default="data/db/telemetry.sqlite",
        help="SQLite database path. Defaults to data/db/telemetry.sqlite.",
    )

    export_csv_parser = subparsers.add_parser("export-csv", help="Export SQLite raw samples into CSV files.")
    export_scope = export_csv_parser.add_mutually_exclusive_group(required=True)
    export_scope.add_argument("--pod", help="Pod id to export.")
    export_scope.add_argument("--all", action="store_true", help="Export one CSV per pod.")
    export_csv_parser.add_argument("--from", dest="date_from", required=True, help="Start UTC day for export.")
    export_csv_parser.add_argument("--to", dest="date_to", required=True, help="End UTC day for export.")
    export_csv_parser.add_argument("--out", help="Single CSV output path for --pod mode.")
    export_csv_parser.add_argument("--out-dir", help="Output directory for --all mode.")
    export_csv_parser.add_argument(
        "--db-path",
        default="data/db/telemetry.sqlite",
        help="SQLite database path. Defaults to data/db/telemetry.sqlite.",
    )

    import_csv_parser = subparsers.add_parser(
        "import-csv",
        help="Copy historical raw/link CSV files into the SQLite database.",
    )
    import_csv_parser.add_argument(
        "--db-path",
        default="data/db/telemetry.sqlite",
        help="SQLite database path. Defaults to data/db/telemetry.sqlite.",
    )
    import_csv_parser.add_argument(
        "--pod",
        action="append",
        help="Optional pod id to import. Repeat for multiple pods. Defaults to all pods found in CSV history.",
    )
    import_csv_parser.add_argument(
        "--skip-link-quality",
        action="store_true",
        help="Import only sample telemetry rows and skip link-quality CSV backfill.",
    )
    import_csv_parser.add_argument(
        "--skip-legacy-logs",
        action="store_true",
        help="Ignore gateway/logs compatibility CSV files and only import canonical data/raw CSVs.",
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
    elif args.command == "export-csv":
        if args.all and args.out:
            parser.error("export-csv does not allow --out together with --all.")
        if args.pod and args.out_dir:
            parser.error("export-csv does not allow --out-dir together with --pod.")

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

    if args.command == "export-training":
        out_path = Path(args.out) if args.out is not None else storage_paths.training_export_path()
        result = export_training_dataset(
            data_root=storage_paths.root,
            date_from=date.fromisoformat(args.date_from),
            date_to=date.fromisoformat(args.date_to),
            out_path=out_path,
        )
        print(result)
        return 0

    if args.command == "init-db":
        print(init_db(args.db_path))
        return 0

    if args.command == "import-csv":
        result = import_csv_history(
            data_root=storage_paths.root,
            db_path=args.db_path,
            include_link_quality=not args.skip_link_quality,
            include_legacy_logs=not args.skip_legacy_logs,
            pod_ids=args.pod,
        )
        print(
            "samples inserted={inserted} duplicates={duplicates} skipped={skipped} seen={seen}".format(
                inserted=result.sample_rows_inserted,
                duplicates=result.sample_duplicates,
                skipped=result.sample_rows_skipped,
                seen=result.sample_rows_seen,
            )
        )
        if not args.skip_link_quality:
            print(
                "link_quality inserted={inserted} duplicates={duplicates} skipped={skipped} seen={seen}".format(
                    inserted=result.link_rows_inserted,
                    duplicates=result.link_duplicates,
                    skipped=result.link_rows_skipped,
                    seen=result.link_rows_seen,
                )
            )
        return 0

    if args.command == "latest":
        db_path = resolve_db_path(args.db_path)
        if not db_path.exists():
            print(f"Database not found: {db_path}")
            return 1
        row = latest_sample(pod_id=args.pod, db_path=db_path)
        if row is None:
            print(f"No samples found for pod {args.pod}")
            return 1
        print(
            "ts_pc_utc={ts} pod_id={pod} seq={seq} temp_c={temp} rh_pct={rh} source={source}".format(
                ts=row["ts_pc_utc"],
                pod=row["pod_id"],
                seq=row["seq"],
                temp=row["temp_c"],
                rh=row["rh_pct"],
                source=row.get("source") or "-",
            )
        )
        return 0

    date_from = date.fromisoformat(args.date_from)
    date_to = date.fromisoformat(args.date_to)
    db_path = resolve_db_path(args.db_path)
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        return 1
    if args.all:
        outputs = export_all_pods_csv(
            date_from=date_from,
            date_to=date_to,
            out_dir=args.out_dir,
            db_path=db_path,
        )
        for output in outputs:
            print(output)
        return 0

    output = export_pod_csv(
        pod_id=args.pod,
        date_from=date_from,
        date_to=date_to,
        out_path=args.out,
        db_path=db_path,
    )
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(cli())
