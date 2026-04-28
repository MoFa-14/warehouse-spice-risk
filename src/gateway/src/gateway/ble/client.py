# File overview:
# - Responsibility: BLE session management for pod scanning, connect/reconnect, and
#   notifications.
# - Project role: Handles BLE discovery, connection, and GATT interaction for the
#   physical pod path.
# - Main data or concerns: BLE addresses, characteristics, notifications, and
#   connection state.
# - Related flow: Receives BLE-facing configuration or requests and passes transport
#   results to ingestion.
# - Why this matters: The physical pod path depends on this layer to convert radio
#   interaction into usable gateway events.

"""BLE session management for pod scanning, connect/reconnect, and notifications."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Callable

from bleak import BleakClient

from gateway.ble.gatt import GattProfile, ensure_profile_present, read_status_text, write_control_command
from gateway.ble.scanner import resolve_device
from gateway.config import GatewaySettings, normalize_address
from gateway.link.stats import LinkStats
from gateway.protocol.decoder import DecodeError, StatusRecord, TelemetryRecord, decode_status_payload, decode_telemetry_payload
from gateway.protocol.validation import validate_telemetry
from gateway.protocol.json_reassembler import JsonReassembler
from gateway.utils.backoff import ExponentialBackoff
from gateway.utils.timeutils import utc_now, utc_now_iso


LOGGER = logging.getLogger(__name__)
SampleHandler = Callable[[TelemetryRecord, tuple[str, ...], LinkStats, str], Awaitable[None]]
CorruptHandler = Callable[[], Awaitable[None]]
ConnectHandler = Callable[[bool], Awaitable[None]]
DisconnectHandler = Callable[[], Awaitable[None]]
# Class purpose: Pod identity used by the gateway supervisor.
# - Project role: Belongs to the gateway BLE transport layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: The physical pod path depends on this layer to convert
#   radio interaction into usable gateway events.
# - Related flow: Receives BLE-facing configuration or requests and passes transport
#   results to ingestion.

@dataclass(frozen=True)
class PodTarget:
    """Pod identity used by the gateway supervisor."""

    address: str
    name: str
# Class purpose: Manage the BLE lifecycle for one pod, including reconnects.
# - Project role: Belongs to the gateway BLE transport layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: The physical pod path depends on this layer to convert
#   radio interaction into usable gateway events.
# - Related flow: Receives BLE-facing configuration or requests and passes transport
#   results to ingestion.

class PodSession:
    """Manage the BLE lifecycle for one pod, including reconnects."""
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: Arguments such as target, settings, profile, sample_handler,
    #   corrupt_handler, connect_handler, disconnect_handler, interpreted
    #   according to the rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    def __init__(
        self,
        *,
        target: PodTarget,
        settings: GatewaySettings,
        profile: GattProfile,
        sample_handler: SampleHandler,
        corrupt_handler: CorruptHandler | None = None,
        connect_handler: ConnectHandler | None = None,
        disconnect_handler: DisconnectHandler | None = None,
    ) -> None:
        self.target = target
        self.settings = settings
        self.profile = profile
        self.sample_handler = sample_handler
        self.corrupt_handler = corrupt_handler
        self.connect_handler = connect_handler
        self.disconnect_handler = disconnect_handler
        self.stats = LinkStats(pod_label=target.name or target.address)
        self._client: BleakClient | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event = asyncio.Event()
        self._disconnected_event = asyncio.Event()
        self._notification_lock = asyncio.Lock()
        self._reassembler = JsonReassembler()
        self._seen_sequences: set[tuple[str, int]] = set()
        self._connected_since_utc: datetime | None = None
        self._last_telemetry_time_utc: datetime | None = None
        self._watchdog_resubscribe_issued_at: datetime | None = None
        self._watchdog_reconnect_requested_at: datetime | None = None
        self._logger = logging.getLogger(f"{__name__}.{normalize_address(target.address).replace(':', '')}")
    # Method purpose: Run the reconnect loop until the gateway is stopped.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

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
                self._logger.debug("Connecting to %s (%s)", scan_match.name, scan_match.address)
                await client.connect(timeout=self.settings.scan_timeout_s)
                ensure_profile_present(client, self.profile)
                self.stats.mark_connected()
                self._connected_since_utc = utc_now()
                self._reset_watchdog_state(clear_telemetry=False)
                backoff.reset()
                self._logger.debug("Connected to %s (%s)", scan_match.name, scan_match.address)
                if self.connect_handler is not None:
                    await self.connect_handler(self.stats.reconnect_count > 0)

                await client.start_notify(self.profile.telemetry_char_uuid, self._handle_notification)
                await self._synchronize_runtime(client)
                watchdog_task = asyncio.create_task(
                    self._telemetry_watchdog_loop(),
                    name=f"telemetry-watchdog-{normalize_address(self.target.address)}",
                )
                try:
                    await self._wait_until_disconnect_or_stop(watchdog_task)
                finally:
                    watchdog_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await watchdog_task
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._logger.error(
                    "BLE session error for %s",
                    self.target.address,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
            finally:
                await self._disconnect_client()

            if self._stop_event.is_set():
                break

            delay = backoff.next_delay()
            self._logger.debug("Scheduling reconnect to %s in %.1fs", self.target.address, delay)
            await self._sleep_or_stop(delay)
    # Method purpose: Request a clean shutdown of the session.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    async def stop(self) -> None:
        """Request a clean shutdown of the session."""
        self._stop_event.set()
        self._disconnected_event.set()
        await self._disconnect_client()
    # Method purpose: Best-effort RSSI refresh using a short scan.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

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
    # Method purpose: Implements the synchronize runtime step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: Arguments such as client, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    async def _synchronize_runtime(self, client: BleakClient) -> None:
        status = await self._read_status(client)
        if status is not None:
            self._log_status_record(status)

        command_sent = await self._maybe_send_command(client)
        interval_updated = await self._maybe_enforce_sample_interval(client, status)

        if command_sent or interval_updated:
            await asyncio.sleep(0.25)
            refreshed_status = await self._read_status(client)
            if refreshed_status is not None and refreshed_status != status:
                self._log_status_record(refreshed_status)
    # Method purpose: Reads status from the relevant storage or runtime source.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: Arguments such as client, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns StatusRecord | None when the function completes
    #   successfully.
    # - Important decisions: The transformation rules here define how later code
    #   interprets the same data, so the shape of the output needs to stay
    #   stable and reproducible.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    async def _read_status(self, client: BleakClient) -> StatusRecord | None:
        try:
            status_text = await read_status_text(client, self.profile)
            return decode_status_payload(status_text)
        except DecodeError as exc:
            self._logger.warning("Status parse warning for %s: %s", self.target.address, exc)
        except Exception as exc:
            self._logger.warning("Status read warning for %s: %s", self.target.address, exc)
        return None
    # Method purpose: Implements the log status record step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: Arguments such as status, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    def _log_status_record(self, status: StatusRecord) -> None:
        self._logger.debug(
            "status firmware_version=%s last_error=%s sample_interval_s=%s",
            status.firmware_version,
            status.last_error,
            status.sample_interval_s,
        )
    # Method purpose: Implements the maybe send command step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: Arguments such as client, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns bool when the function completes successfully.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    async def _maybe_send_command(self, client: BleakClient) -> bool:
        if not self.settings.send_command:
            return False
        command = self.settings.send_command.strip()
        if not command:
            return False
        await write_control_command(client, self.profile, command)
        self._logger.debug("control command sent: %s", command)
        return True
    # Method purpose: Implements the maybe enforce sample interval step used by
    #   this subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: Arguments such as client, status, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: Returns bool when the function completes successfully.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    async def _maybe_enforce_sample_interval(self, client: BleakClient, status: StatusRecord | None) -> bool:
        desired_interval_s = int(self.settings.sample_interval_s)
        if desired_interval_s <= 0:
            return False
        if self._has_explicit_interval_command():
            return False
        if status is not None and status.sample_interval_s == desired_interval_s:
            return False

        command = f"SET_INTERVAL:{desired_interval_s}"
        await write_control_command(client, self.profile, command)
        if status is None:
            self._logger.debug(
                "control command sent: %s (status unavailable, enforcing configured interval)",
                command,
            )
        else:
            self._logger.debug(
                "control command sent: %s (pod reported %ss)",
                command,
                status.sample_interval_s,
            )
        return True
    # Method purpose: Implements the has explicit interval command step used by
    #   this subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns bool when the function completes successfully.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    def _has_explicit_interval_command(self) -> bool:
        if not self.settings.send_command:
            return False
        return self.settings.send_command.strip().upper().startswith("SET_INTERVAL")
    # Method purpose: Implements the handle notification step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: Arguments such as _characteristic, payload, interpreted
    #   according to the rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    async def _handle_notification(self, _characteristic, payload: bytearray) -> None:
        async with self._notification_lock:
            messages = self._reassembler.feed_bytes(payload)
            for message in messages:
                try:
                    record = decode_telemetry_payload(message)
                except DecodeError as exc:
                    self._logger.warning("Discarding malformed telemetry from %s: %s", self.target.address, exc)
                    if self.corrupt_handler is not None:
                        await self.corrupt_handler()
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

                # Only unique accepted samples should refresh the watchdog timer.
                # Repeated duplicates can otherwise keep the BLE session "alive"
                # while storage and the dashboard stop advancing.
                sample_seen_at = utc_now()
                self._last_telemetry_time_utc = sample_seen_at
                timestamp = utc_now_iso(sample_seen_at)
                missing = self.stats.note_received(
                    seq=record.seq,
                    ts_uptime_s=record.ts_uptime_s,
                    seen_time_utc=timestamp,
                )
                if missing:
                    quality_flags.append("seq_gap")

                ordered_flags = tuple(dict.fromkeys(quality_flags))
                self._seen_sequences.add(dedupe_key)
                self._logger.debug(
                    "telemetry pod_id=%s seq=%s ts_uptime_s=%.1f temp_c=%s rh_pct=%s flags=%s quality_flags=%s",
                    record.pod_id,
                    record.seq,
                    record.ts_uptime_s,
                    record.temp_c,
                    record.rh_pct,
                    record.flags,
                    "|".join(ordered_flags) if ordered_flags else "-",
                )
                try:
                    await self.sample_handler(record, ordered_flags, self.stats, timestamp)
                except Exception as exc:
                    self._logger.error(
                        "Telemetry sample handling failed for pod_id=%s seq=%s",
                        record.pod_id,
                        record.seq,
                        exc_info=(type(exc), exc, exc.__traceback__),
                    )
    # Method purpose: Implements the wait until disconnect or stop step used by
    #   this subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: Arguments such as *extra_tasks, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    async def _wait_until_disconnect_or_stop(self, *extra_tasks: asyncio.Task[None]) -> None:
        stop_wait = asyncio.create_task(self._stop_event.wait())
        disconnect_wait = asyncio.create_task(self._disconnected_event.wait())
        wait_set = {stop_wait, disconnect_wait, *extra_tasks}
        done, pending = await asyncio.wait(wait_set, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for task in done:
            task.result()
    # Method purpose: Implements the sleep or stop step used by this subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: Arguments such as delay_s, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    async def _sleep_or_stop(self, delay_s: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay_s)
        except asyncio.TimeoutError:
            return
    # Method purpose: Implements the disconnect client step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    async def _disconnect_client(self) -> None:
        client, self._client = self._client, None
        if client is None:
            self._connected_since_utc = None
            self._reset_watchdog_state()
            return
        if client.is_connected:
            with contextlib.suppress(Exception):
                await client.disconnect()
        self._reassembler.reset()
        self._connected_since_utc = None
        self._reset_watchdog_state()
        self.stats.mark_disconnected()
    # Method purpose: Implements the on disconnected step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: Arguments such as _client, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    def _on_disconnected(self, _client: BleakClient) -> None:
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._handle_disconnected_event)
    # Method purpose: Implements the handle disconnected event step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    def _handle_disconnected_event(self) -> None:
        self._connected_since_utc = None
        self._reset_watchdog_state()
        self.stats.mark_disconnected()
        if self.disconnect_handler is not None and self._loop is not None:
            task = self._loop.create_task(self.disconnect_handler())
            task.add_done_callback(self._log_background_callback_exception)
        self._disconnected_event.set()
        self._logger.debug("Disconnected from %s", self.target.address)
    # Method purpose: Implements the log background callback exception step used
    #   by this subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: Arguments such as task, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    def _log_background_callback_exception(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        try:
            task.result()
        except Exception as exc:
            self._logger.error(
                "Background callback failed for %s",
                self.target.address,
                exc_info=(type(exc), exc, exc.__traceback__),
            )
    # Method purpose: Implements the telemetry watchdog loop step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    async def _telemetry_watchdog_loop(self) -> None:
        while not self._stop_event.is_set() and not self._disconnected_event.is_set():
            await asyncio.sleep(self._watchdog_poll_interval_s())
            await self._telemetry_watchdog_tick()
    # Method purpose: Implements the telemetry watchdog tick step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: Arguments such as now, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    async def _telemetry_watchdog_tick(self, now: datetime | None = None) -> None:
        action = self._determine_watchdog_action(now or utc_now())
        if action == "resubscribe":
            self._logger.warning(
                "No telemetry for %.1fs from %s; forcing notification resubscribe.",
                self._seconds_since_last_telemetry(now or utc_now()),
                self.target.address,
            )
            try:
                await self._force_resubscribe()
            except Exception as exc:
                self._logger.error(
                    "Telemetry watchdog resubscribe failed for %s",
                    self.target.address,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
        elif action == "reconnect":
            self._logger.warning(
                "Telemetry still stalled for %.1fs from %s after resubscribe; reconnecting BLE client.",
                self._seconds_since_last_telemetry(now or utc_now()),
                self.target.address,
            )
            try:
                await self._force_reconnect()
            except Exception as exc:
                self._logger.error(
                    "Telemetry watchdog reconnect failed for %s",
                    self.target.address,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
    # Method purpose: Implements the determine watchdog action step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: Arguments such as now, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns str | None when the function completes successfully.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    def _determine_watchdog_action(self, now: datetime) -> str | None:
        client = self._client
        if client is None or not getattr(client, "is_connected", False) or not self.stats.connected:
            return None

        if (
            self._watchdog_resubscribe_issued_at is not None
            and self._last_telemetry_time_utc is not None
            and self._last_telemetry_time_utc > self._watchdog_resubscribe_issued_at
        ):
            self._watchdog_resubscribe_issued_at = None
            self._watchdog_reconnect_requested_at = None

        reference_time = self._last_telemetry_time_utc or self._connected_since_utc
        if reference_time is None:
            return None

        elapsed_s = (now - reference_time).total_seconds()
        stall_timeout_s = self._telemetry_stall_timeout_s()
        reconnect_timeout_s = stall_timeout_s + self._telemetry_reconnect_timeout_s()

        if self._watchdog_resubscribe_issued_at is None and elapsed_s > stall_timeout_s:
            self._watchdog_resubscribe_issued_at = now
            return "resubscribe"

        if (
            self._watchdog_resubscribe_issued_at is not None
            and self._watchdog_reconnect_requested_at is None
            and elapsed_s > reconnect_timeout_s
        ):
            self._watchdog_reconnect_requested_at = now
            return "reconnect"

        return None
    # Method purpose: Implements the force resubscribe step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    async def _force_resubscribe(self) -> None:
        client = self._client
        if client is None or not getattr(client, "is_connected", False):
            return
        async with self._notification_lock:
            self._reassembler.reset()
            with contextlib.suppress(Exception):
                await client.stop_notify(self.profile.telemetry_char_uuid)
            await client.start_notify(self.profile.telemetry_char_uuid, self._handle_notification)
    # Method purpose: Implements the force reconnect step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    async def _force_reconnect(self) -> None:
        client = self._client
        if client is None:
            return
        if getattr(client, "is_connected", False):
            await client.disconnect()
        self._handle_disconnected_event()
    # Method purpose: Implements the reset watchdog state step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: Arguments such as clear_telemetry, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    def _reset_watchdog_state(self, *, clear_telemetry: bool = True) -> None:
        self._watchdog_resubscribe_issued_at = None
        self._watchdog_reconnect_requested_at = None
        if clear_telemetry:
            self._last_telemetry_time_utc = None
    # Method purpose: Implements the telemetry stall timeout s step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns float when the function completes successfully.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    def _telemetry_stall_timeout_s(self) -> float:
        return (2.0 * float(self.settings.sample_interval_s)) + 5.0
    # Method purpose: Implements the telemetry reconnect timeout s step used by
    #   this subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns float when the function completes successfully.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    def _telemetry_reconnect_timeout_s(self) -> float:
        return max(float(self.settings.sample_interval_s) + 5.0, 5.0)
    # Method purpose: Implements the watchdog poll interval s step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns float when the function completes successfully.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    def _watchdog_poll_interval_s(self) -> float:
        return max(1.0, min(5.0, float(self.settings.sample_interval_s)))
    # Method purpose: Implements the seconds since last telemetry step used by
    #   this subsystem.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: Arguments such as now, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns float when the function completes successfully.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    def _seconds_since_last_telemetry(self, now: datetime) -> float:
        reference_time = self._last_telemetry_time_utc or self._connected_since_utc
        if reference_time is None:
            return 0.0
        return max((now - reference_time).total_seconds(), 0.0)
    # Method purpose: Best-effort resend request written to the pod control
    #   characteristic.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: Arguments such as seq, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    async def request_resend_seq(self, seq: int) -> None:
        """Best-effort resend request written to the pod control characteristic."""
        client = self._client
        if client is None or not getattr(client, "is_connected", False):
            self._logger.debug("BLE resend requested (stub) seq=%s but client is not connected", seq)
            return
        command = f"REQ_SEQ:{int(seq)}"
        await write_control_command(client, self.profile, command)
        self._logger.debug("BLE resend requested (stub) seq=%s", seq)
    # Method purpose: Best-effort resend range request written to the pod
    #   control characteristic.
    # - Project role: Belongs to the gateway BLE transport layer and acts as a
    #   method on PodSession.
    # - Inputs: Arguments such as from_seq, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The physical pod path depends on this layer to
    #   convert radio interaction into usable gateway events.
    # - Related flow: Receives BLE-facing configuration or requests and passes
    #   transport results to ingestion.

    async def request_resend_from_seq(self, from_seq: int) -> None:
        """Best-effort resend range request written to the pod control characteristic."""
        client = self._client
        if client is None or not getattr(client, "is_connected", False):
            self._logger.debug("BLE resend requested (stub) from_seq=%s but client is not connected", from_seq)
            return
        command = f"REQ_FROM_SEQ:{int(from_seq)}"
        await write_control_command(client, self.profile, command)
        self._logger.debug("BLE resend requested (stub) from_seq=%s", from_seq)
