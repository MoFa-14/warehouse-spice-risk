"""BLE session management for pod scanning, connect/reconnect, and notifications."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable

from bleak import BleakClient

from gateway.ble.gatt import GattProfile, ensure_profile_present, read_status_text, write_control_command
from gateway.ble.scanner import resolve_device
from gateway.config import GatewaySettings, normalize_address
from gateway.link.stats import LinkStats
from gateway.protocol.decoder import DecodeError, TelemetryRecord, decode_status_payload, decode_telemetry_payload
from gateway.protocol.json_reassembler import JsonReassembler
from gateway.protocol.validation import validate_telemetry
from gateway.utils.backoff import ExponentialBackoff
from gateway.utils.timeutils import utc_now_iso


LOGGER = logging.getLogger(__name__)
SampleHandler = Callable[[TelemetryRecord, tuple[str, ...], LinkStats, str], Awaitable[None]]


@dataclass(frozen=True)
class PodTarget:
    """Pod identity used by the gateway supervisor."""

    address: str
    name: str


class PodSession:
    """Manage the BLE lifecycle for one pod, including reconnects."""

    def __init__(
        self,
        *,
        target: PodTarget,
        settings: GatewaySettings,
        profile: GattProfile,
        sample_handler: SampleHandler,
    ) -> None:
        self.target = target
        self.settings = settings
        self.profile = profile
        self.sample_handler = sample_handler
        self.stats = LinkStats(pod_label=target.name or target.address)
        self._client: BleakClient | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event = asyncio.Event()
        self._disconnected_event = asyncio.Event()
        self._notification_lock = asyncio.Lock()
        self._reassembler = JsonReassembler()
        self._seen_sequences: set[tuple[str, int]] = set()
        self._logger = logging.getLogger(f"{__name__}.{normalize_address(target.address).replace(':', '')}")

    async def run(self) -> None:
        """Run the reconnect loop until the gateway is stopped."""
        self._loop = asyncio.get_running_loop()
        backoff = ExponentialBackoff()

        while not self._stop_event.is_set():
            self._disconnected_event.clear()
            scan_match = await resolve_device(
                timeout=self.settings.scan_timeout_s,
                name_prefix=self.settings.device_name_scan_prefix,
                service_uuid=self.profile.service_uuid,
                address=self.target.address,
            )
            if scan_match is None:
                delay = backoff.next_delay()
                self._logger.warning("Pod %s not found; retrying in %.1fs", self.target.address, delay)
                await self._sleep_or_stop(delay)
                continue

            self.stats.update_rssi(scan_match.rssi)
            client = BleakClient(
                scan_match.ble_device,
                disconnected_callback=self._on_disconnected,
                services=[self.profile.service_uuid],
                winrt={"use_cached_services": self.settings.use_cached_services},
            )
            self._client = client

            try:
                self._logger.info("Connecting to %s (%s)", scan_match.name, scan_match.address)
                await client.connect(timeout=self.settings.scan_timeout_s)
                ensure_profile_present(client, self.profile)
                self.stats.mark_connected()
                backoff.reset()
                self._logger.info("Connected to %s (%s)", scan_match.name, scan_match.address)

                await self._maybe_send_command(client)
                await self._log_status(client)
                await client.start_notify(self.profile.telemetry_char_uuid, self._handle_notification)
                await self._wait_until_disconnect_or_stop()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._logger.warning("BLE session error for %s: %s", self.target.address, exc)
            finally:
                await self._disconnect_client()

            if self._stop_event.is_set():
                break

            delay = backoff.next_delay()
            self._logger.info("Scheduling reconnect to %s in %.1fs", self.target.address, delay)
            await self._sleep_or_stop(delay)

    async def stop(self) -> None:
        """Request a clean shutdown of the session."""
        self._stop_event.set()
        self._disconnected_event.set()
        await self._disconnect_client()

    async def refresh_rssi(self) -> None:
        """Best-effort RSSI refresh using a short scan."""
        scan_match = await resolve_device(
            timeout=min(5.0, self.settings.scan_timeout_s),
            name_prefix=self.settings.device_name_scan_prefix,
            service_uuid=self.profile.service_uuid,
            address=self.target.address,
        )
        if scan_match is not None:
            self.stats.update_rssi(scan_match.rssi)

    async def _log_status(self, client: BleakClient) -> None:
        status_text = await read_status_text(client, self.profile)
        try:
            status = decode_status_payload(status_text)
        except DecodeError as exc:
            self._logger.warning("Status parse warning for %s: %s | raw=%s", self.target.address, exc, status_text)
            return

        self._logger.info(
            "status firmware_version=%s last_error=%s sample_interval_s=%s",
            status.firmware_version,
            status.last_error,
            status.sample_interval_s,
        )

    async def _maybe_send_command(self, client: BleakClient) -> None:
        if not self.settings.send_command:
            return
        command = self.settings.send_command.strip()
        if not command:
            return
        await write_control_command(client, self.profile, command)
        self._logger.info("control command sent: %s", command)

    async def _handle_notification(self, _characteristic, payload: bytearray) -> None:
        async with self._notification_lock:
            messages = self._reassembler.feed_bytes(payload)
            for message in messages:
                try:
                    record = decode_telemetry_payload(message)
                except DecodeError as exc:
                    self._logger.warning("Discarding malformed telemetry from %s: %s", self.target.address, exc)
                    continue

                self.stats.update_identity(record.pod_id)
                quality_flags: list[str] = []

                if self.stats.should_reset_sequence(seq=record.seq, ts_uptime_s=record.ts_uptime_s):
                    self.stats.reset_sequence_tracking()
                    self._seen_sequences.clear()
                    quality_flags.append("sequence_reset")

                dedupe_key = (record.pod_id, record.seq)
                if dedupe_key in self._seen_sequences:
                    self.stats.note_duplicate()
                    self._logger.debug("Duplicate telemetry ignored for pod_id=%s seq=%s", record.pod_id, record.seq)
                    continue

                validation = validate_telemetry(
                    record,
                    temp_min_c=self.settings.validation.temp_min_c,
                    temp_max_c=self.settings.validation.temp_max_c,
                    firmware=self.settings.firmware,
                )
                quality_flags.extend(validation.quality_flags)

                timestamp = utc_now_iso()
                missing = self.stats.note_received(
                    seq=record.seq,
                    ts_uptime_s=record.ts_uptime_s,
                    seen_time_utc=timestamp,
                )
                if missing:
                    quality_flags.append("seq_gap")

                ordered_flags = tuple(dict.fromkeys(quality_flags))
                self._seen_sequences.add(dedupe_key)
                self._logger.info(
                    "telemetry pod_id=%s seq=%s ts_uptime_s=%.1f temp_c=%s rh_pct=%s flags=%s quality_flags=%s",
                    record.pod_id,
                    record.seq,
                    record.ts_uptime_s,
                    record.temp_c,
                    record.rh_pct,
                    record.flags,
                    "|".join(ordered_flags) if ordered_flags else "-",
                )
                await self.sample_handler(record, ordered_flags, self.stats, timestamp)

    async def _wait_until_disconnect_or_stop(self) -> None:
        stop_wait = asyncio.create_task(self._stop_event.wait())
        disconnect_wait = asyncio.create_task(self._disconnected_event.wait())
        done, pending = await asyncio.wait(
            {stop_wait, disconnect_wait},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for task in done:
            task.result()

    async def _sleep_or_stop(self, delay_s: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay_s)
        except asyncio.TimeoutError:
            return

    async def _disconnect_client(self) -> None:
        client, self._client = self._client, None
        if client is None:
            return
        if client.is_connected:
            with contextlib.suppress(Exception):
                await client.disconnect()
        self.stats.mark_disconnected()

    def _on_disconnected(self, _client: BleakClient) -> None:
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._handle_disconnected_event)

    def _handle_disconnected_event(self) -> None:
        self.stats.mark_disconnected()
        self._disconnected_event.set()
        self._logger.warning("Disconnected from %s", self.target.address)
