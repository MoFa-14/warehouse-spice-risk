"""Measure end-to-end latency through the TCP -> gateway -> SQLite -> dashboard path."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import socket
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

import sys


ROOT = Path(__file__).resolve().parents[2]
for package_root in (ROOT / "src" / "gateway" / "src", ROOT / "src" / "dashboard"):
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))

from app.main import create_app
from gateway.config import ValidationSettings
from gateway.firmware_config_loader import default_firmware_config_path, load_firmware_config
from gateway.ingesters.tcp_ingester import TcpIngester, TcpIngesterSettings
from gateway.multi.record import TelemetryRecord
from gateway.multi.router import PodRouter
from werkzeug.serving import make_server


UTC = timezone.utc
DEFAULT_RESULTS_DIR = ROOT / "evaluation" / "results"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Measure gateway/database/dashboard latency using the TCP pod path.")
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR, help="Directory for evaluation outputs.")
    parser.add_argument("--db-path", type=Path, help="Temporary SQLite database used for the latency run.")
    parser.add_argument("--timing-log", type=Path, help="JSONL file used for opt-in gateway acceptance timestamps.")
    parser.add_argument("--sample-count", type=int, default=50, help="Number of sequential samples to measure.")
    parser.add_argument("--tcp-port", type=int, default=8876, help="TCP port for the gateway ingester.")
    parser.add_argument("--dashboard-port", type=int, default=5051, help="HTTP port for the dashboard app.")
    parser.add_argument("--pod-id", default="99", help="Pod id used for the latency probe.")
    parser.add_argument("--poll-interval-ms", type=int, default=25, help="Polling cadence for DB/API checks.")
    parser.add_argument("--timeout-seconds", type=float, default=10.0, help="Per-sample timeout for each stage.")
    return parser.parse_args()


class LoopThread:
    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run, name="latency-eval-loop", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def run(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result()

    def stop(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=5.0)


class GatewayHarness:
    def __init__(self, *, db_path: Path, tcp_port: int) -> None:
        self.queue: asyncio.Queue[TelemetryRecord] = asyncio.Queue(maxsize=1000)
        firmware = load_firmware_config(default_firmware_config_path())
        self.router = PodRouter(
            queue=self.queue,
            firmware=firmware,
            validation=ValidationSettings(temp_min_c=-20.0, temp_max_c=80.0),
            storage_backend="sqlite",
            data_root=ROOT / "src" / "data",
            db_path=db_path,
        )
        self.ingester = TcpIngester(
            queue=self.queue,
            router=self.router,
            settings=TcpIngesterSettings(host="127.0.0.1", port=tcp_port),
        )

    async def start(self) -> None:
        self.router.start()
        await self.ingester.start()

    async def stop(self) -> None:
        await self.ingester.stop()
        await self.router.stop()


class DashboardServer:
    def __init__(self, *, db_path: Path, port: int, runtime_dir: Path) -> None:
        app = create_app(
            {
                "TESTING": False,
                "DATA_ROOT": ROOT / "src" / "data",
                "DB_PATH": db_path,
                "RUNTIME_DIR": runtime_dir,
                "ACKS_FILE": runtime_dir / "acks.json",
                "SECRET_KEY": "latency-evaluation",
                "AUTO_REFRESH_SECONDS": 0,
            }
        )
        self.server = make_server("127.0.0.1", port, app)
        self.thread = threading.Thread(target=self.server.serve_forever, name="latency-dashboard", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.thread.join(timeout=5.0)


def main() -> int:
    args = parse_args()
    if args.sample_count < 30:
        raise SystemExit("--sample-count must be at least 30 for the requested latency evaluation.")
    args.results_dir.mkdir(parents=True, exist_ok=True)
    db_path = args.db_path or (args.results_dir / "latency_telemetry.sqlite")
    timing_log = args.timing_log or (args.results_dir / "latency_gateway_events.jsonl")
    runtime_dir = args.results_dir / "dashboard_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    _reset_path(db_path)
    _reset_path(Path(f"{db_path}-shm"))
    _reset_path(Path(f"{db_path}-wal"))
    _reset_path(Path(f"{db_path}.lock"))
    _reset_path(timing_log)

    os.environ["DSP_EVAL_TIMING_LOG"] = str(timing_log)
    loop_thread = LoopThread()
    dashboard = DashboardServer(db_path=db_path, port=args.dashboard_port, runtime_dir=runtime_dir)

    loop_thread.start()
    gateway = loop_thread.run(_build_gateway_harness(db_path=db_path, tcp_port=args.tcp_port))
    loop_thread.run(gateway.start())
    dashboard.start()
    _wait_for_http(f"http://127.0.0.1:{args.dashboard_port}/", timeout_s=args.timeout_seconds)

    records: list[dict[str, Any]] = []
    try:
        with socket.create_connection(("127.0.0.1", args.tcp_port), timeout=args.timeout_seconds) as sock:
            sock.settimeout(args.timeout_seconds)
            for seq in range(1, args.sample_count + 1):
                t0 = _utc_now()
                temp_c = 20.0 + (seq / 100.0)
                rh_pct = 45.0 + (seq / 100.0)
                payload = {
                    "pod_id": args.pod_id,
                    "seq": seq,
                    "ts_uptime_s": float(seq),
                    "temp_c": temp_c,
                    "rh_pct": rh_pct,
                    "flags": 0,
                    "eval_src_utc": _iso_micro(t0),
                }
                sock.sendall((json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8"))

                gateway_event = _wait_for_gateway_event(
                    timing_log_path=timing_log,
                    pod_id=args.pod_id,
                    seq=seq,
                    timeout_s=args.timeout_seconds,
                    poll_interval_s=args.poll_interval_ms / 1000.0,
                )
                row, t2 = _wait_for_db_row(
                    db_path=db_path,
                    pod_id=args.pod_id,
                    seq=seq,
                    timeout_s=args.timeout_seconds,
                    poll_interval_s=args.poll_interval_ms / 1000.0,
                )
                api_payload, t3 = _wait_for_dashboard_visibility(
                    dashboard_port=args.dashboard_port,
                    pod_id=args.pod_id,
                    expected_temp_c=temp_c,
                    expected_rh_pct=rh_pct,
                    timeout_s=args.timeout_seconds,
                    poll_interval_s=args.poll_interval_ms / 1000.0,
                )

                t1 = _parse_utc(gateway_event["ts_gateway_accepted_utc"])
                gateway_latency_ms = _delta_ms(t0, t1)
                db_latency_ms = _delta_ms(t0, t2)
                dashboard_latency_ms = _delta_ms(t0, t3)
                db_after_gateway_ms = _delta_ms(t1, t2)
                end_to_end_ms = dashboard_latency_ms

                records.append(
                    {
                        "sample_id": f"{args.pod_id}:{seq}",
                        "pod_id": args.pod_id,
                        "seq": seq,
                        "t0_src_utc": _iso_micro(t0),
                        "t1_gateway_accepted_utc": gateway_event["ts_gateway_accepted_utc"],
                        "t1_row_ts_pc_utc": row["ts_pc_utc"],
                        "t2_db_visible_utc": _iso_micro(t2),
                        "t3_dashboard_visible_utc": _iso_micro(t3),
                        "temp_c": temp_c,
                        "rh_pct": rh_pct,
                        "gateway_ingestion_ms": gateway_latency_ms,
                        "db_availability_ms": db_latency_ms,
                        "db_after_gateway_ms": db_after_gateway_ms,
                        "dashboard_visibility_ms": dashboard_latency_ms,
                        "end_to_end_ms": end_to_end_ms,
                        "dashboard_status": api_payload.get("status"),
                    }
                )
    finally:
        os.environ.pop("DSP_EVAL_TIMING_LOG", None)
        dashboard.stop()
        loop_thread.run(gateway.stop())
        loop_thread.stop()

    records_path = args.results_dir / "latency_records.csv"
    summary_path = args.results_dir / "latency_summary.json"
    _write_csv(records_path, records)
    summary = {
        "sample_count": len(records),
        "pod_id": args.pod_id,
        "tcp_port": args.tcp_port,
        "dashboard_port": args.dashboard_port,
        "metrics": {
            "median_gateway_ingestion_ms": median(record["gateway_ingestion_ms"] for record in records),
            "median_db_availability_ms": median(record["db_availability_ms"] for record in records),
            "median_db_after_gateway_ms": median(record["db_after_gateway_ms"] for record in records),
            "median_dashboard_visibility_ms": median(record["dashboard_visibility_ms"] for record in records),
            "worst_end_to_end_ms": max(record["end_to_end_ms"] for record in records),
        },
        "records_path": str(records_path),
        "timing_log_path": str(timing_log),
        "db_path": str(db_path),
        "measurement_path": "Synthetic/TCP probe -> gateway TCP ingester -> router -> SQLite -> dashboard JSON route",
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


def _reset_path(path: Path) -> None:
    if path.exists():
        path.unlink()


async def _build_gateway_harness(*, db_path: Path, tcp_port: int) -> GatewayHarness:
    return GatewayHarness(db_path=db_path, tcp_port=tcp_port)


def _wait_for_http(url: str, *, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.0):
                return
        except urllib.error.URLError:
            time.sleep(0.05)
    raise TimeoutError(f"Timed out waiting for HTTP endpoint {url}")


def _wait_for_gateway_event(
    *,
    timing_log_path: Path,
    pod_id: str,
    seq: int,
    timeout_s: float,
    poll_interval_s: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if timing_log_path.exists():
            with timing_log_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    payload = json.loads(line)
                    if payload.get("event") != "gateway_accepted":
                        continue
                    if str(payload.get("pod_id")) == str(pod_id) and int(payload.get("seq")) == int(seq):
                        return payload
        time.sleep(poll_interval_s)
    raise TimeoutError(f"Timed out waiting for gateway acceptance event pod={pod_id} seq={seq}")


def _wait_for_db_row(
    *,
    db_path: Path,
    pod_id: str,
    seq: int,
    timeout_s: float,
    poll_interval_s: float,
) -> tuple[dict[str, Any], datetime]:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if db_path.exists():
            connection = sqlite3.connect(db_path)
            connection.row_factory = sqlite3.Row
            try:
                row = connection.execute(
                    """
                    SELECT ts_pc_utc, pod_id, seq, temp_c, rh_pct
                    FROM samples_raw
                    WHERE pod_id = ? AND seq = ?
                    """,
                    (str(pod_id), int(seq)),
                ).fetchone()
            finally:
                connection.close()
            if row is not None:
                return dict(row), _utc_now()
        time.sleep(poll_interval_s)
    raise TimeoutError(f"Timed out waiting for SQLite row pod={pod_id} seq={seq}")


def _wait_for_dashboard_visibility(
    *,
    dashboard_port: int,
    pod_id: str,
    expected_temp_c: float,
    expected_rh_pct: float,
    timeout_s: float,
    poll_interval_s: float,
) -> tuple[dict[str, Any], datetime]:
    deadline = time.monotonic() + timeout_s
    url = f"http://127.0.0.1:{dashboard_port}/api/pods/{pod_id}/latest"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.0) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code != 404:
                raise
            payload = None
        except urllib.error.URLError:
            payload = None
        if payload is not None:
            if _close_enough(payload.get("temp_c"), expected_temp_c) and _close_enough(payload.get("rh_pct"), expected_rh_pct):
                return payload, _utc_now()
        time.sleep(poll_interval_s)
    raise TimeoutError(f"Timed out waiting for dashboard visibility pod={pod_id} temp={expected_temp_c} rh={expected_rh_pct}")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _close_enough(value: Any, expected: float) -> bool:
    try:
        return abs(float(value) - float(expected)) < 1e-9
    except (TypeError, ValueError):
        return False


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)


def _iso_micro(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _delta_ms(start: datetime, end: datetime) -> float:
    return (end - start).total_seconds() * 1000.0


if __name__ == "__main__":
    raise SystemExit(main())
