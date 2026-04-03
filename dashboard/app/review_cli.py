"""CLI entrypoint for lightweight monitoring review summaries."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Sequence

from app.config import DashboardConfig
from app.services.review_service import build_monitoring_review_context
from app.services.timeseries_service import resolve_time_window
from app.timezone import resolve_display_timezone


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Warehouse Spice Risk monitoring review summary")
    parser.add_argument("--range", default="7d", help="Review range key such as 24h or 7d. Defaults to 7d.")
    parser.add_argument("--start", help="Optional custom start datetime-local value in the dashboard display timezone.")
    parser.add_argument("--end", help="Optional custom end datetime-local value in the dashboard display timezone.")
    parser.add_argument("--pod", help="Optional pod id to summarize.")
    parser.add_argument("--db-path", default=str(DashboardConfig.DB_PATH), help="SQLite database path.")
    parser.add_argument("--data-root", default=str(DashboardConfig.DATA_ROOT), help="Dashboard data root.")
    parser.add_argument("--acks-file", default=str(DashboardConfig.ACKS_FILE), help="Acknowledgement file path.")
    return parser.parse_args(argv)


def cli(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    display_timezone = resolve_display_timezone(DashboardConfig.DISPLAY_TIMEZONE)
    window = resolve_time_window(args.range, args.start, args.end, display_timezone=display_timezone)
    context = build_monitoring_review_context(
        Path(args.data_root),
        window=window,
        db_path=Path(args.db_path),
        pod_id=args.pod,
        acks_file=Path(args.acks_file),
    )
    serializable = {
        "generated_at": context["generated_at"].isoformat(),
        "window": {
            "key": context["window"].key,
            "start": context["window"].start.isoformat(),
            "end": context["window"].end.isoformat(),
            "custom": context["window"].custom,
        },
        "scope_label": context["scope_label"],
        "selected_pod_id": context["selected_pod_id"],
        "summary": context["summary"],
        "rows": [asdict(row) for row in context["rows"]],
    }
    print(json.dumps(serializable, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(cli())
