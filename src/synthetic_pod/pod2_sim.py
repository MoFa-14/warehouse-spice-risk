# File overview:
# - Responsibility: Synthetic pod simulator that connects to the gateway over TCP
#   and supports replay.
# - Project role: Defines executable simulation entry points and supporting
#   simulation behavior.
# - Main data or concerns: Simulation configuration, generated telemetry, and
#   runtime control state.
# - Related flow: Wraps simulation helpers into runnable synthetic pod behavior.
# - Why this matters: Entry scripts matter because they determine how the synthetic
#   environment is exposed to the rest of the system.

"""Synthetic pod simulator that connects to the gateway over TCP and supports replay."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import random
from datetime import datetime
from types import SimpleNamespace
from typing import Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sim.buffer import ReplayBuffer
from sim.faults import FaultAction, FaultController, FaultProfile
from sim.generator import SyntheticTelemetryGenerator
from sim.schedule import ActiveHoursSchedule
from sim.zone_profiles import get_zone_profile, zone_profile_names


LOGGER = logging.getLogger("synthetic_pod")
# Function purpose: Parses start local into structured values.
# - Project role: Belongs to the synthetic pod runtime layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns datetime | None when the function completes successfully.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Wraps simulation helpers into runnable synthetic pod behavior.

def _parse_start_local(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid --start-local value {value!r}. Expected an ISO timestamp such as 2026-07-15T09:00:00."
        ) from exc
# Function purpose: Validates timezone before it enters later storage or analysis.
# - Project role: Belongs to the synthetic pod runtime layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns str when the function completes successfully.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Wraps simulation helpers into runnable synthetic pod behavior.

def _validate_timezone(value: str) -> str:
    value = str(value).strip()
    if not value:
        raise argparse.ArgumentTypeError("--timezone must not be empty.")
    try:
        ZoneInfo(value)
    except ZoneInfoNotFoundError:
        # Some Windows Python environments do not ship the IANA timezone database.
        # We still accept the timezone name and fall back to a named local-clock mode.
        if "/" not in value and value.upper() != "UTC":
            raise argparse.ArgumentTypeError(
                f"Unknown timezone {value!r}. Example valid value: Europe/London."
            ) from None
    return value
# Function purpose: Normalizes pod identifier into the subsystem's stable
#   representation.
# - Project role: Belongs to the synthetic pod runtime layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns str when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Wraps simulation helpers into runnable synthetic pod behavior.

def _normalize_pod_id(value: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError("Pod id must not be empty.")
    if text.isdigit():
        return text.zfill(max(2, len(text)))
    return text
# Function purpose: Parses explicit pod ids into structured values.
# - Project role: Belongs to the synthetic pod runtime layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns list[str] when the function completes successfully.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Wraps simulation helpers into runnable synthetic pod behavior.

def _parse_explicit_pod_ids(value: str) -> list[str]:
    pod_ids = [_normalize_pod_id(part) for part in str(value).split(",") if str(part).strip()]
    if not pod_ids:
        raise ValueError("--pod-ids must include at least one non-empty pod id.")
    duplicates = {pod_id for pod_id in pod_ids if pod_ids.count(pod_id) > 1}
    if duplicates:
        duplicate_list = ", ".join(sorted(duplicates))
        raise ValueError(f"--pod-ids contains duplicate pod ids: {duplicate_list}.")
    return pod_ids
# Function purpose: Resolves pod ids into the concrete value used later.
# - Project role: Belongs to the synthetic pod runtime layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as args, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns list[str] when the function completes successfully.
# - Important decisions: Entry scripts matter because they determine how the
#   synthetic environment is exposed to the rest of the system.
# - Related flow: Wraps simulation helpers into runnable synthetic pod behavior.

def resolve_pod_ids(args: argparse.Namespace) -> list[str]:
    explicit_ids = str(getattr(args, "pod_ids", "") or "").strip()
    if explicit_ids:
        return _parse_explicit_pod_ids(explicit_ids)

    pod_count = int(getattr(args, "pod_count", 1) or 1)
    if pod_count <= 0:
        raise ValueError("--pod-count must be positive.")

    if pod_count == 1:
        return [_normalize_pod_id(getattr(args, "pod_id", "02"))]

    start_text = _normalize_pod_id(getattr(args, "pod_id_start", "02"))
    if not start_text.isdigit():
        raise ValueError("--pod-id-start must be numeric when --pod-count is greater than 1.")

    width = max(2, len(start_text))
    start_value = int(start_text)
    return [f"{start_value + offset:0{width}d}" for offset in range(pod_count)]
# Function purpose: Builds pod args for the next stage of the project flow.
# - Project role: Belongs to the synthetic pod runtime layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as args, pod_id, index, interpreted according to the
#   rules encoded in the body below.
# - Outputs: Returns argparse.Namespace when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Wraps simulation helpers into runnable synthetic pod behavior.

def build_pod_args(args: argparse.Namespace, *, pod_id: str, index: int) -> argparse.Namespace:
    seed_base = getattr(args, "seed_base", None)
    seed = None if seed_base is None else int(seed_base) + int(index)
    cloned = dict(vars(args))
    cloned["pod_id"] = str(pod_id)
    cloned["seed"] = seed
    return SimpleNamespace(**cloned)
# Function purpose: Parses args into structured values.
# - Project role: Belongs to the synthetic pod runtime layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as argv, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns argparse.Namespace when the function completes successfully.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Wraps simulation helpers into runnable synthetic pod behavior.

def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synthetic pod simulator for multi-pod gateway tests")
    parser.add_argument("--gateway-host", default="127.0.0.1")
    parser.add_argument("--gateway-port", type=int, default=8765)
    parser.add_argument("--pod-id", default="02")
    parser.add_argument(
        "--pod-ids",
        help="Optional comma-separated list of explicit pod ids, for example 02,03,04.",
    )
    parser.add_argument(
        "--pod-count",
        type=int,
        default=1,
        help="Number of sequential pod ids to launch starting from --pod-id-start. Defaults to 1.",
    )
    parser.add_argument(
        "--pod-id-start",
        default="02",
        help="First sequential pod id when --pod-count is greater than 1.",
    )
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument(
        "--seed-base",
        type=int,
        help="Optional deterministic seed base. Each pod uses seed-base + its cluster index.",
    )
    parser.add_argument(
        "--timezone",
        type=_validate_timezone,
        default="Europe/London",
        help="IANA timezone used for Bristol-style day/night and seasonal alignment.",
    )
    parser.add_argument(
        "--start-local",
        type=_parse_start_local,
        help="Optional ISO local timestamp that anchors the simulation clock, for example 2026-07-15T09:00:00.",
    )
    parser.add_argument(
        "--zone-profile",
        choices=zone_profile_names(),
        default="entrance_disturbed",
        help="Built-in warehouse micro-zone profile for pod 02.",
    )
    parser.add_argument("--base-temp", type=float, help="Override the zone baseline temperature in C.")
    parser.add_argument("--base-rh", type=float, help="Override the zone baseline relative humidity in percent.")
    parser.add_argument("--noise-temp", type=float, help="Temperature noise standard deviation in C.")
    parser.add_argument("--noise-rh", type=float, help="Relative humidity noise standard deviation in percent.")
    parser.add_argument("--drift-temp", type=float, help="Temperature drift random-walk step in C per sample.")
    parser.add_argument("--drift-rh", type=float, help="Relative humidity drift random-walk step in percent per sample.")
    parser.add_argument("--event-rate", type=float, help="Baseline disturbance event rate per hour.")
    parser.add_argument(
        "--event-rate-active-hours",
        type=float,
        help="Disturbance event rate per hour during active hours. Defaults to the zone profile.",
    )
    parser.add_argument("--event-spike-temp", type=float, help="Temperature spike size in C during disturbance events.")
    parser.add_argument("--event-spike-rh", type=float, help="Relative humidity spike size in percent during disturbance events.")
    parser.add_argument("--recovery-tau-seconds", type=float, help="Exponential recovery time constant after a disturbance.")
    parser.add_argument("--active-hours-start", type=int, default=8, help="Start hour for active warehouse operations.")
    parser.add_argument("--active-hours-end", type=int, default=18, help="End hour for active warehouse operations.")
    parser.add_argument("--p-drop", type=float, default=0.0)
    parser.add_argument("--p-corrupt", type=float, default=0.0)
    parser.add_argument("--p-delay", type=float, default=0.0)
    parser.add_argument("--p-disconnect", type=float, default=0.0)
    parser.add_argument("--max-delay", type=float, default=5.0)
    parser.add_argument("--disconnect-min", type=float, default=2.0)
    parser.add_argument("--disconnect-max", type=float, default=10.0)
    parser.add_argument(
        "--burst-loss",
        choices=("on", "off"),
        default="off",
        help="Enable or disable bursty loss/delay periods during disturbances.",
    )
    parser.add_argument("--burst-duration-seconds", type=float, default=30.0)
    parser.add_argument("--burst-multiplier", type=float, default=3.0)
    parser.add_argument("--replay-buffer-size", type=int, default=300)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    if args.interval <= 0:
        parser.error("--interval must be positive.")
    if args.gateway_port <= 0:
        parser.error("--gateway-port must be positive.")
    if args.replay_buffer_size <= 0:
        parser.error("--replay-buffer-size must be positive.")
    if args.pod_count <= 0:
        parser.error("--pod-count must be positive.")
    if not 0 <= args.active_hours_start <= 23 or not 0 <= args.active_hours_end <= 23:
        parser.error("--active-hours-start and --active-hours-end must be between 0 and 23.")
    for flag_name in ("p_drop", "p_corrupt", "p_delay", "p_disconnect"):
        value = getattr(args, flag_name)
        if value < 0.0 or value > 1.0:
            parser.error(f"--{flag_name.replace('_', '-')} must be between 0 and 1.")
    for flag_name in (
        "max_delay",
        "disconnect_min",
        "disconnect_max",
        "noise_temp",
        "noise_rh",
        "drift_temp",
        "drift_rh",
        "event_rate",
        "event_rate_active_hours",
        "event_spike_temp",
        "event_spike_rh",
        "recovery_tau_seconds",
        "burst_duration_seconds",
        "burst_multiplier",
    ):
        value = getattr(args, flag_name)
        if value is not None and value < 0.0:
            parser.error(f"--{flag_name.replace('_', '-')} must be non-negative.")
    try:
        resolve_pod_ids(args)
    except ValueError as exc:
        parser.error(str(exc))
    return args
# Function purpose: Implements the configure logging step used by this subsystem.
# - Project role: Belongs to the synthetic pod runtime layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as verbose, interpreted according to the rules encoded in
#   the body below.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: Entry scripts matter because they determine how the
#   synthetic environment is exposed to the rest of the system.
# - Related flow: Wraps simulation helpers into runnable synthetic pod behavior.

def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
# Class purpose: Generate telemetry, inject faults, and replay missing samples on
#   demand.
# - Project role: Belongs to the synthetic pod runtime layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Entry scripts matter because they determine how the
#   synthetic environment is exposed to the rest of the system.
# - Related flow: Wraps simulation helpers into runnable synthetic pod behavior.

class SyntheticPodClient:
    """Generate telemetry, inject faults, and replay missing samples on demand."""
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the synthetic pod runtime layer and acts as a
    #   method on SyntheticPodClient.
    # - Inputs: Arguments such as args, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Wraps simulation helpers into runnable synthetic pod
    #   behavior.

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        zone_profile = get_zone_profile(args.zone_profile)
        generator_seed = getattr(args, "seed", None)
        generator_rng = None if generator_seed is None else random.Random(int(generator_seed))
        fault_rng = random.Random() if generator_seed is None else random.Random(int(generator_seed) ^ 0x5F3759DF)
        schedule = ActiveHoursSchedule(
            active_start_hour=args.active_hours_start,
            active_end_hour=args.active_hours_end,
        )
        self.generator = SyntheticTelemetryGenerator.from_zone_profile(
            pod_id=args.pod_id,
            interval_s=args.interval,
            zone_profile=zone_profile,
            base_temp_c=args.base_temp,
            base_rh_pct=args.base_rh,
            noise_temp_c=args.noise_temp,
            noise_rh_pct=args.noise_rh,
            drift_temp_step_c=args.drift_temp,
            drift_rh_step_pct=args.drift_rh,
            event_rate_per_hour=args.event_rate,
            event_rate_active_hours_per_hour=args.event_rate_active_hours,
            event_spike_temp_c=args.event_spike_temp,
            event_spike_rh_pct=args.event_spike_rh,
            recovery_tau_seconds=args.recovery_tau_seconds,
            timezone_name=args.timezone,
            start_local_time=args.start_local,
            schedule=schedule,
            rng=generator_rng,
        )
        self.buffer = ReplayBuffer(maxlen=args.replay_buffer_size)
        self.faults = FaultController(
            profile=FaultProfile(
                p_drop=args.p_drop,
                p_corrupt=args.p_corrupt,
                p_delay=args.p_delay,
                p_disconnect=args.p_disconnect,
                max_delay_s=args.max_delay,
                disconnect_min_s=args.disconnect_min,
                disconnect_max_s=args.disconnect_max,
                burst_loss_enabled=args.burst_loss == "on",
                burst_duration_s=args.burst_duration_seconds,
                burst_multiplier=args.burst_multiplier,
            ),
            interval_s=args.interval,
            rng=fault_rng,
        )
        self._stop_event = asyncio.Event()
        self._pending_deliveries: set[asyncio.Task[None]] = set()
        self._writer: asyncio.StreamWriter | None = None
    # Method purpose: Implements the run step used by this subsystem.
    # - Project role: Belongs to the synthetic pod runtime layer and acts as a
    #   method on SyntheticPodClient.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns int when the function completes successfully.
    # - Important decisions: Entry scripts matter because they determine how the
    #   synthetic environment is exposed to the rest of the system.
    # - Related flow: Wraps simulation helpers into runnable synthetic pod
    #   behavior.

    async def run(self) -> int:
        while not self._stop_event.is_set():
            try:
                reader, writer = await asyncio.open_connection(self.args.gateway_host, self.args.gateway_port)
            except OSError as exc:
                LOGGER.warning("[pod=%s] Gateway connection failed: %s. Retrying in 2s.", self.args.pod_id, exc)
                await asyncio.sleep(2.0)
                continue

            self._writer = writer
            LOGGER.info("[pod=%s] Connected to gateway at %s:%s", self.args.pod_id, self.args.gateway_host, self.args.gateway_port)
            command_task = asyncio.create_task(self._command_loop(reader), name=f"synthetic-command-loop-{self.args.pod_id}")
            try:
                disconnect_requested = await self._send_loop()
            finally:
                command_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await command_task
                await self._close_writer()

            if disconnect_requested > 0:
                LOGGER.warning("[pod=%s] Simulated disconnect for %.1fs before reconnecting.", self.args.pod_id, disconnect_requested)
                await asyncio.sleep(disconnect_requested)
            else:
                await asyncio.sleep(1.0)
        return 0
    # Method purpose: Implements the send loop step used by this subsystem.
    # - Project role: Belongs to the synthetic pod runtime layer and acts as a
    #   method on SyntheticPodClient.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns float when the function completes successfully.
    # - Important decisions: Entry scripts matter because they determine how the
    #   synthetic environment is exposed to the rest of the system.
    # - Related flow: Wraps simulation helpers into runnable synthetic pod
    #   behavior.

    async def _send_loop(self) -> float:
        while not self._stop_event.is_set():
            sample = self.generator.next_sample()
            payload = sample.to_payload()
            self.buffer.add(payload)
            action = self.faults.choose_action(disturbance_active=sample.disturbance_active)
            LOGGER.info(
                "[pod=%s zone=%s] seq=%s temp=%.3f rh=%.3f disturbance=%s event=%s active_hours=%s burst_fault=%s",
                sample.pod_id,
                sample.zone_profile,
                sample.seq,
                sample.temp_c,
                sample.rh_pct,
                "on" if sample.disturbance_active else "off",
                "triggered" if sample.disturbance_just_triggered else "steady",
                "yes" if sample.active_hours else "no",
                "yes" if action.burst_active else "no",
            )

            if action.disconnect_s > 0:
                return action.disconnect_s
            if action.drop:
                LOGGER.info(
                    "Dropped seq=%s on purpose for fault injection (burst=%s effective_drop=%.2f effective_delay=%.2f).",
                    sample.seq,
                    "on" if action.burst_active else "off",
                    action.effective_p_drop,
                    action.effective_p_delay,
                )
            elif action.delay_s > 0:
                task = asyncio.create_task(
                    self._deliver_after_delay(payload, action),
                    name=f"delay-seq-{self.args.pod_id}-{sample.seq}",
                )
                self._pending_deliveries.add(task)
                task.add_done_callback(self._pending_deliveries.discard)
            else:
                await self._deliver_sample(payload, corrupt=action.corrupt)

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.args.interval)
            except asyncio.TimeoutError:
                continue

        return 0.0
    # Method purpose: Implements the deliver after delay step used by this
    #   subsystem.
    # - Project role: Belongs to the synthetic pod runtime layer and acts as a
    #   method on SyntheticPodClient.
    # - Inputs: Arguments such as sample, action, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Entry scripts matter because they determine how the
    #   synthetic environment is exposed to the rest of the system.
    # - Related flow: Wraps simulation helpers into runnable synthetic pod
    #   behavior.

    async def _deliver_after_delay(self, sample: dict[str, object], action: FaultAction) -> None:
        await asyncio.sleep(action.delay_s)
        LOGGER.info("Delayed seq=%s by %.2fs", sample["seq"], action.delay_s)
        await self._deliver_sample(sample, corrupt=action.corrupt)
    # Method purpose: Implements the deliver sample step used by this subsystem.
    # - Project role: Belongs to the synthetic pod runtime layer and acts as a
    #   method on SyntheticPodClient.
    # - Inputs: Arguments such as sample, corrupt, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Entry scripts matter because they determine how the
    #   synthetic environment is exposed to the rest of the system.
    # - Related flow: Wraps simulation helpers into runnable synthetic pod
    #   behavior.

    async def _deliver_sample(self, sample: dict[str, object], *, corrupt: bool) -> None:
        writer = self._writer
        if writer is None or writer.is_closing():
            LOGGER.warning("Skipping send for seq=%s because the gateway connection is not available.", sample["seq"])
            return

        if corrupt:
            payload = self._corrupt_payload(sample)
            LOGGER.warning("Sending corrupt payload for seq=%s", sample["seq"])
        else:
            payload = json.dumps(sample, separators=(",", ":")) + "\n"

        writer.write(payload.encode("utf-8"))
        await writer.drain()
    # Method purpose: Implements the command loop step used by this subsystem.
    # - Project role: Belongs to the synthetic pod runtime layer and acts as a
    #   method on SyntheticPodClient.
    # - Inputs: Arguments such as reader, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Entry scripts matter because they determine how the
    #   synthetic environment is exposed to the rest of the system.
    # - Related flow: Wraps simulation helpers into runnable synthetic pod
    #   behavior.

    async def _command_loop(self, reader: asyncio.StreamReader) -> None:
        while not self._stop_event.is_set():
            line = await reader.readline()
            if not line:
                return
            command = json.loads(line.decode("utf-8", errors="replace").strip())
            cmd = str(command.get("cmd", "")).upper()
            if cmd == "REQ_SEQ":
                seq = int(command["seq"])
                replay = self.buffer.get(seq)
                if replay is not None:
                    LOGGER.info("Replaying seq=%s", seq)
                    await self._deliver_sample(replay, corrupt=False)
            elif cmd == "REQ_FROM_SEQ":
                from_seq = int(command["from_seq"])
                replayed = 0
                for sample in self.buffer.iter_from_seq(from_seq):
                    await self._deliver_sample(sample, corrupt=False)
                    replayed += 1
                LOGGER.info("Replayed %s sample(s) from seq=%s", replayed, from_seq)
    # Method purpose: Implements the close writer step used by this subsystem.
    # - Project role: Belongs to the synthetic pod runtime layer and acts as a
    #   method on SyntheticPodClient.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Entry scripts matter because they determine how the
    #   synthetic environment is exposed to the rest of the system.
    # - Related flow: Wraps simulation helpers into runnable synthetic pod
    #   behavior.

    async def _close_writer(self) -> None:
        for task in list(self._pending_deliveries):
            task.cancel()
        for task in list(self._pending_deliveries):
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._pending_deliveries.clear()

        if self._writer is not None:
            with contextlib.suppress(Exception):
                self._writer.close()
                await self._writer.wait_closed()
        self._writer = None
    # Method purpose: Implements the corrupt payload step used by this
    #   subsystem.
    # - Project role: Belongs to the synthetic pod runtime layer and acts as a
    #   method on SyntheticPodClient.
    # - Inputs: Arguments such as sample, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns str when the function completes successfully.
    # - Important decisions: Entry scripts matter because they determine how the
    #   synthetic environment is exposed to the rest of the system.
    # - Related flow: Wraps simulation helpers into runnable synthetic pod
    #   behavior.

    @staticmethod
    def _corrupt_payload(sample: dict[str, object]) -> str:
        good = json.dumps(sample, separators=(",", ":"))
        mode = random.choice(("truncate", "garble", "invalid"))
        if mode == "truncate":
            cut = max(1, len(good) // 2)
            return good[:cut] + "\n"
        if mode == "garble":
            return good[:-2] + "??\n"
        return good.replace("{", "", 1) + "\n"
# Function purpose: Implements the async main step used by this subsystem.
# - Project role: Belongs to the synthetic pod runtime layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as args, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns int when the function completes successfully.
# - Important decisions: Entry scripts matter because they determine how the
#   synthetic environment is exposed to the rest of the system.
# - Related flow: Wraps simulation helpers into runnable synthetic pod behavior.

async def async_main(args: argparse.Namespace) -> int:
    pod_ids = resolve_pod_ids(args)
    clients = [
        SyntheticPodClient(build_pod_args(args, pod_id=pod_id, index=index))
        for index, pod_id in enumerate(pod_ids)
    ]
    LOGGER.info("Starting %s synthetic pod client(s): %s", len(clients), ", ".join(pod_ids))
    tasks = [
        asyncio.create_task(client.run(), name=f"synthetic-pod-{client.args.pod_id}")
        for client in clients
    ]
    try:
        results = await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    except Exception:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    return max(results, default=0)
# Function purpose: Implements the CLI step used by this subsystem.
# - Project role: Belongs to the synthetic pod runtime layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as argv, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns int when the function completes successfully.
# - Important decisions: Entry scripts matter because they determine how the
#   synthetic environment is exposed to the rest of the system.
# - Related flow: Wraps simulation helpers into runnable synthetic pod behavior.

def cli(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(cli())
