# File overview:
# - Responsibility: CLI entrypoint for the concurrent multi-pod gateway test mode.
# - Project role: Exposes operator-facing commands for gateway runtime, storage, and
#   forecasting tasks.
# - Main data or concerns: CLI arguments, runtime options, and command outcomes.
# - Related flow: Receives command-line arguments and dispatches to gateway
#   services.
# - Why this matters: Operational workflows remain reproducible when the CLI
#   documents the same runtime paths used elsewhere.

"""CLI entrypoint for the concurrent multi-pod gateway test mode."""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
from typing import Sequence

from gateway.config import parse_addresses
from gateway.multi.orchestrator import MultiGatewayOrchestrator, MultiGatewaySettings


LOGGER = logging.getLogger(__name__)
# Function purpose: Parses args into structured values.
# - Project role: Belongs to the gateway CLI entry-point layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as argv, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns argparse.Namespace when the function completes successfully.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Receives command-line arguments and dispatches to gateway
#   services.

def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Warehouse Spice Risk gateway multi-pod tools")
    subparsers = parser.add_subparsers(dest="command", required=True)

    multi = subparsers.add_parser("multi", help="Run concurrent BLE + TCP multi-pod ingestion.")
    multi.add_argument("--ble-address", action="append", help="Optional BLE address for the real hardware pod.")
    multi.add_argument("--ble-name-prefix", default="SHT45-POD-", help="BLE scan prefix for hardware pods.")
    multi.add_argument("--tcp-port", type=int, default=8765, help="TCP listener port for synthetic pods.")
    multi.add_argument("--duration", type=float, help="Optional runtime duration in seconds.")
    multi.add_argument("--log-root", default="data/raw", help="Canonical raw storage root.")
    multi.add_argument(
        "--storage",
        choices=("sqlite", "csv"),
        default="sqlite",
        help="Primary storage backend. Defaults to sqlite.",
    )
    multi.add_argument(
        "--db-path",
        default="data/db/telemetry.sqlite",
        help="SQLite database path used when --storage sqlite.",
    )
    multi.add_argument("--interval-s", type=int, default=60, help="Expected sample interval for watchdog timing.")
    multi.add_argument("--scan-timeout", type=float, default=10.0, help="BLE scan timeout in seconds.")
    multi.add_argument("--rssi-poll-interval", type=float, default=30.0, help="Best-effort RSSI refresh cadence.")
    multi.add_argument("--temp-min-c", type=float, default=-20.0)
    multi.add_argument("--temp-max-c", type=float, default=80.0)
    multi.add_argument("--firmware-config", help="Optional path override for firmware/circuitpython-pod/config.py.")
    multi.add_argument("--use-cached-services", action="store_true")
    multi.add_argument("--verbose", action="store_true")

    args = parser.parse_args(argv)
    if args.command == "multi":
        if args.tcp_port <= 0:
            parser.error("--tcp-port must be positive.")
        if args.interval_s <= 0:
            parser.error("--interval-s must be positive.")
        if args.duration is not None and args.duration <= 0:
            parser.error("--duration must be positive when provided.")
        if args.temp_min_c >= args.temp_max_c:
            parser.error("--temp-min-c must be lower than --temp-max-c.")
    return args
# Function purpose: Implements the configure logging step used by this subsystem.
# - Project role: Belongs to the gateway CLI entry-point layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as verbose, interpreted according to the rules encoded in
#   the body below.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: Operational workflows remain reproducible when the CLI
#   documents the same runtime paths used elsewhere.
# - Related flow: Receives command-line arguments and dispatches to gateway
#   services.

def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("gateway").setLevel(logging.DEBUG if verbose else logging.INFO)
    logging.getLogger("bleak").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
# Function purpose: Implements the async main step used by this subsystem.
# - Project role: Belongs to the gateway CLI entry-point layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as args, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns int when the function completes successfully.
# - Important decisions: Operational workflows remain reproducible when the CLI
#   documents the same runtime paths used elsewhere.
# - Related flow: Receives command-line arguments and dispatches to gateway
#   services.

async def async_main(args: argparse.Namespace) -> int:
    if args.command != "multi":
        raise ValueError(f"Unsupported command: {args.command}")

    settings = MultiGatewaySettings(
        firmware_config_path=args.firmware_config,
        ble_addresses=parse_addresses(args.ble_address),
        ble_name_prefix=args.ble_name_prefix,
        tcp_port=args.tcp_port,
        duration_s=args.duration,
        log_root=Path(args.log_root),
        storage_backend=args.storage,
        db_path=Path(args.db_path),
        interval_s=args.interval_s,
        temp_min_c=args.temp_min_c,
        temp_max_c=args.temp_max_c,
        scan_timeout_s=args.scan_timeout,
        rssi_poll_interval_s=args.rssi_poll_interval,
        use_cached_services=args.use_cached_services,
    )
    runtime = MultiGatewayOrchestrator(settings)
    return await runtime.run()
# Function purpose: Implements the CLI step used by this subsystem.
# - Project role: Belongs to the gateway CLI entry-point layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as argv, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns int when the function completes successfully.
# - Important decisions: Operational workflows remain reproducible when the CLI
#   documents the same runtime paths used elsewhere.
# - Related flow: Receives command-line arguments and dispatches to gateway
#   services.

def cli(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(getattr(args, "verbose", False))
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    try:
        raise SystemExit(cli())
    except KeyboardInterrupt:
        LOGGER.info("Stopped by user.")
        raise SystemExit(130)
