"""CLI entrypoint for the Layer 2 BLE gateway with Layer 3 storage enabled."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
from collections.abc import Awaitable
from typing import Sequence

from bleak import BleakClient

from gateway.ble.client import PodSession, PodTarget
from gateway.ble.gatt import ensure_profile_present, iter_service_lines, profile_from_firmware
from gateway.ble.scanner import discover_matches, resolve_device
from gateway.config import GatewaySettings, build_settings
from gateway.logging.process_lock import GatewayProcessLock
from gateway.logging.writer_pipeline import GatewayWriterPipeline
from gateway.protocol.decoder import TelemetryRecord
from gateway.storage import build_storage_paths
from gateway.utils.timeutils import utc_now_iso


LOGGER = logging.getLogger(__name__)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the gateway CLI arguments."""
    parser = argparse.ArgumentParser(description="Warehouse Spice Risk BLE gateway")
    parser.add_argument("--scan-only", action="store_true", help="Scan for matching pods and exit.")
    parser.add_argument("--dump-services", action="store_true", help="Connect to one pod and print its GATT services.")
    parser.add_argument(
        "--address",
        action="append",
        help="Specific BLE address to connect to. Repeat or comma-separate for multiple pods.",
    )
    parser.add_argument("--duration", type=float, help="Run for N seconds, then exit. Omit to run until stopped.")
    parser.add_argument("--log-dir", default="gateway/logs", help="Directory for samples.csv and link_quality.csv.")
    parser.add_argument("--send", help="Optional control command stub, for example PING or REQ_FROM_SEQ:123.")
    parser.add_argument("--scan-timeout", type=float, default=10.0, help="BLE scan timeout in seconds.")
    parser.add_argument("--metrics-interval", type=float, default=30.0, help="Link snapshot cadence in seconds.")
    parser.add_argument(
        "--rssi-poll-interval",
        type=float,
        default=30.0,
        help="Best-effort RSSI refresh cadence in seconds.",
    )
    parser.add_argument("--temp-min-c", type=float, default=-20.0, help="Minimum reasonable temperature.")
    parser.add_argument("--temp-max-c", type=float, default=80.0, help="Maximum reasonable temperature.")
    parser.add_argument(
        "--firmware-config",
        help="Override the path to firmware/circuitpython-pod/config.py. Defaults to the repo source of truth.",
    )
    parser.add_argument(
        "--use-cached-services",
        action="store_true",
        help="Allow Windows to use cached GATT services instead of forcing a fresh read.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    args = parser.parse_args(argv)

    if args.scan_only and args.dump_services:
        parser.error("--scan-only and --dump-services cannot be used together.")
    if args.metrics_interval <= 0:
        parser.error("--metrics-interval must be greater than 0.")
    if args.rssi_poll_interval <= 0:
        parser.error("--rssi-poll-interval must be greater than 0.")
    if args.temp_min_c >= args.temp_max_c:
        parser.error("--temp-min-c must be lower than --temp-max-c.")
    if args.duration is not None and args.duration <= 0:
        parser.error("--duration must be greater than 0 when provided.")

    return args


def configure_logging(verbose: bool) -> None:
    """Set up a concise console logger for the gateway runtime."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


async def resolve_initial_targets(settings: GatewaySettings) -> list[PodTarget]:
    """Resolve the pod set to monitor for this run."""
    profile = profile_from_firmware(settings.firmware)

    if settings.addresses:
        matches = await discover_matches(
            timeout=settings.scan_timeout_s,
            name_prefix=settings.device_name_scan_prefix,
            service_uuid=profile.service_uuid,
            addresses=settings.addresses,
        )
        match_by_address = {match.address: match for match in matches}
        targets = []
        for address in settings.addresses:
            match = match_by_address.get(address)
            name = match.name if match is not None else address
            targets.append(PodTarget(address=address, name=name))
        return targets

    matches = await discover_matches(
        timeout=settings.scan_timeout_s,
        name_prefix=settings.device_name_scan_prefix,
        service_uuid=profile.service_uuid,
    )
    return [PodTarget(address=match.address, name=match.name) for match in matches]


async def run_scan_only(settings: GatewaySettings) -> int:
    """Scan for matching pods and print the results."""
    profile = profile_from_firmware(settings.firmware)
    matches = await discover_matches(
        timeout=settings.scan_timeout_s,
        name_prefix=settings.device_name_scan_prefix,
        service_uuid=profile.service_uuid,
        addresses=settings.addresses,
    )

    LOGGER.info("Using firmware config: %s", settings.firmware.config_path)
    LOGGER.info("Scanning for pods with prefix %s", settings.device_name_scan_prefix)
    if not matches:
        LOGGER.warning("No matching pods found.")
        return 1

    for match in matches:
        services = ",".join(match.service_uuids) if match.service_uuids else "-"
        print(
            f"FOUND name={match.name} address={match.address} rssi={match.rssi if match.rssi is not None else 'n/a'} services={services}"
        )
    return 0


async def run_dump_services(settings: GatewaySettings) -> int:
    """Connect to one pod and print its discovered services."""
    profile = profile_from_firmware(settings.firmware)
    targets = await resolve_initial_targets(settings)
    if not targets:
        LOGGER.warning("No matching pods found for service dump.")
        return 1

    target = targets[0]
    match = await resolve_device(
        timeout=settings.scan_timeout_s,
        name_prefix=settings.device_name_scan_prefix,
        service_uuid=profile.service_uuid,
        address=target.address,
    )
    if match is None:
        LOGGER.warning("Could not resolve target %s for service dump.", target.address)
        return 1

    client = BleakClient(
        match.ble_device,
        services=[profile.service_uuid],
        winrt={"use_cached_services": settings.use_cached_services},
    )
    try:
        await client.connect(timeout=settings.scan_timeout_s)
        ensure_profile_present(client, profile)
        for line in iter_service_lines(client):
            print(line)
        return 0
    finally:
        if client.is_connected:
            with contextlib.suppress(Exception):
                await client.disconnect()


class GatewayRuntime:
    """Coordinate pod sessions, periodic RSSI refreshes, and CSV logging."""

    def __init__(self, settings: GatewaySettings) -> None:
        self.settings = settings
        self.profile = profile_from_firmware(settings.firmware)
        self.storage_paths = build_storage_paths()
        self.writer_pipeline: GatewayWriterPipeline | None = None
        self.process_lock = GatewayProcessLock(self.settings.log_dir / ".lock")
        self.sessions: list[PodSession] = []
        self._stop_event = asyncio.Event()
        self._fatal_task_error: BaseException | None = None

    async def run(self, duration_s: float | None) -> int:
        """Run the receiver until the duration elapses or the user stops it."""
        LOGGER.info("Using firmware config: %s", self.settings.firmware.config_path)
        LOGGER.info("Legacy logs will be written to %s", self.settings.log_dir)
        LOGGER.info("Canonical Layer 3 data root: %s", self.storage_paths.root)
        LOGGER.info("Requested pod sample interval: %ss", self.settings.firmware.sample_interval_s)
        try:
            self.process_lock.acquire()
        except RuntimeError as exc:
            LOGGER.error("%s", exc)
            return 1

        try:
            session_tasks: list[asyncio.Task[None]] = []
            snapshot_task: asyncio.Task[None] | None = None
            rssi_task: asyncio.Task[None] | None = None

            targets = await resolve_initial_targets(self.settings)
            if not targets:
                LOGGER.warning("No matching pods found to connect.")
                return 1

            self.writer_pipeline = GatewayWriterPipeline(
                storage_root=self.storage_paths.root,
                log_dir=self.settings.log_dir,
            )
            self.writer_pipeline.start()
            self.sessions = [
                PodSession(
                    target=target,
                    settings=self.settings,
                    profile=self.profile,
                    sample_handler=self.handle_sample,
                )
                for target in targets
            ]
            LOGGER.info("Starting %s pod session(s)", len(self.sessions))

            session_tasks = [
                self._create_guarded_task(session.run(), name=f"pod-session-{index}")
                for index, session in enumerate(self.sessions, start=1)
            ]
            snapshot_task = self._create_guarded_task(self.link_snapshot_loop(), name="link-snapshot-loop")
            rssi_task = self._create_guarded_task(self.rssi_poll_loop(), name="rssi-poll-loop")

            if duration_s is None:
                await self._stop_event.wait()
            else:
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=duration_s)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass
        finally:
            self._stop_event.set()
            for session in self.sessions:
                await session.stop()
            for background_task in (snapshot_task, rssi_task):
                if background_task is not None:
                    background_task.cancel()
            for background_task in (snapshot_task, rssi_task):
                if background_task is not None:
                    with contextlib.suppress(asyncio.CancelledError):
                        await background_task
            if session_tasks:
                await asyncio.gather(*session_tasks, return_exceptions=True)
            await self.log_link_snapshots()
            if self.writer_pipeline is not None:
                await self.writer_pipeline.stop()
            self.process_lock.release()

        return 1 if self._fatal_task_error is not None else 0

    async def handle_sample(
        self,
        record: TelemetryRecord,
        quality_flags: tuple[str, ...],
        stats,
        timestamp: str,
    ) -> None:
        """Persist a decoded sample into both canonical and compatibility outputs."""
        if self.writer_pipeline is None:
            return

        await self.writer_pipeline.enqueue_sample(
            ts_pc_utc=timestamp,
            record=record,
            rssi=stats.last_rssi,
            quality_flags=quality_flags,
        )

    async def link_snapshot_loop(self) -> None:
        """Write periodic link metrics snapshots for every pod."""
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.settings.metrics_interval_s)
            except asyncio.TimeoutError:
                await self.log_link_snapshots()

    async def rssi_poll_loop(self) -> None:
        """Refresh last known RSSI values when the adapter exposes them."""
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.settings.rssi_poll_interval_s)
            except asyncio.TimeoutError:
                results = await asyncio.gather(
                    *(session.refresh_rssi() for session in self.sessions),
                    return_exceptions=True,
                )
                for session, result in zip(self.sessions, results):
                    if isinstance(result, Exception):
                        LOGGER.debug("RSSI refresh failed for %s: %s", session.target.address, result)

    async def log_link_snapshots(self) -> None:
        """Write one link metrics snapshot row per active pod."""
        if self.writer_pipeline is None:
            return
        timestamp = utc_now_iso()
        for session in self.sessions:
            snapshot = session.stats.snapshot(ts_pc_utc=timestamp)
            await self.writer_pipeline.enqueue_link_snapshot(snapshot)

    def _create_guarded_task(self, awaitable: Awaitable[None], *, name: str) -> asyncio.Task[None]:
        task = asyncio.create_task(awaitable, name=name)

        def _on_done(completed: asyncio.Task[None]) -> None:
            if completed.cancelled():
                return
            try:
                completed.result()
            except Exception as exc:
                LOGGER.critical(
                    "Background task %s crashed.",
                    name,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
                self._fatal_task_error = exc
                self._stop_event.set()
            else:
                if not self._stop_event.is_set():
                    LOGGER.error("Background task %s exited unexpectedly.", name)
                    self._fatal_task_error = RuntimeError(f"{name} exited unexpectedly")
                    self._stop_event.set()

        task.add_done_callback(_on_done)
        return task


async def async_main(args: argparse.Namespace) -> int:
    """Async CLI entrypoint used by the module and helper tools."""
    settings = build_settings(
        firmware_config_path=args.firmware_config,
        log_dir=args.log_dir,
        addresses=args.address,
        scan_timeout_s=args.scan_timeout,
        metrics_interval_s=args.metrics_interval,
        rssi_poll_interval_s=args.rssi_poll_interval,
        temp_min_c=args.temp_min_c,
        temp_max_c=args.temp_max_c,
        send_command=args.send,
        use_cached_services=args.use_cached_services,
    )

    if args.scan_only:
        return await run_scan_only(settings)
    if args.dump_services:
        return await run_dump_services(settings)

    runtime = GatewayRuntime(settings)
    return await runtime.run(args.duration)


def cli(argv: Sequence[str] | None = None) -> int:
    """Synchronous CLI wrapper for python -m gateway.main."""
    args = parse_args(argv)
    configure_logging(args.verbose)
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    try:
        raise SystemExit(cli())
    except KeyboardInterrupt:
        LOGGER.info("Stopped by user.")
        raise SystemExit(130)
