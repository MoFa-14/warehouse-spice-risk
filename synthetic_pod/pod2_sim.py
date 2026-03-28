"""Synthetic pod 02 that connects to the gateway over TCP and supports replay."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import random
from typing import Sequence

from sim.buffer import ReplayBuffer
from sim.faults import FaultAction, FaultController, FaultProfile
from sim.generator import SyntheticTelemetryGenerator
from sim.schedule import ActiveHoursSchedule
from sim.zone_profiles import get_zone_profile, zone_profile_names


LOGGER = logging.getLogger("synthetic_pod")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synthetic pod simulator for multi-pod gateway tests")
    parser.add_argument("--gateway-host", default="127.0.0.1")
    parser.add_argument("--gateway-port", type=int, default=8765)
    parser.add_argument("--pod-id", default="02")
    parser.add_argument("--interval", type=int, default=10)
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
    return args


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


class SyntheticPodClient:
    """Generate telemetry, inject faults, and replay missing samples on demand."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        zone_profile = get_zone_profile(args.zone_profile)
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
            schedule=schedule,
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
        )
        self._stop_event = asyncio.Event()
        self._pending_deliveries: set[asyncio.Task[None]] = set()
        self._writer: asyncio.StreamWriter | None = None

    async def run(self) -> int:
        while not self._stop_event.is_set():
            try:
                reader, writer = await asyncio.open_connection(self.args.gateway_host, self.args.gateway_port)
            except OSError as exc:
                LOGGER.warning("Gateway connection failed: %s. Retrying in 2s.", exc)
                await asyncio.sleep(2.0)
                continue

            self._writer = writer
            LOGGER.info("Connected to gateway at %s:%s", self.args.gateway_host, self.args.gateway_port)
            command_task = asyncio.create_task(self._command_loop(reader), name="synthetic-command-loop")
            try:
                disconnect_requested = await self._send_loop()
            finally:
                command_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await command_task
                await self._close_writer()

            if disconnect_requested > 0:
                LOGGER.warning("Simulated disconnect for %.1fs before reconnecting.", disconnect_requested)
                await asyncio.sleep(disconnect_requested)
            else:
                await asyncio.sleep(1.0)
        return 0

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
                task = asyncio.create_task(self._deliver_after_delay(payload, action), name=f"delay-seq-{sample.seq}")
                self._pending_deliveries.add(task)
                task.add_done_callback(self._pending_deliveries.discard)
            else:
                await self._deliver_sample(payload, corrupt=action.corrupt)

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.args.interval)
            except asyncio.TimeoutError:
                continue

        return 0.0

    async def _deliver_after_delay(self, sample: dict[str, object], action: FaultAction) -> None:
        await asyncio.sleep(action.delay_s)
        LOGGER.info("Delayed seq=%s by %.2fs", sample["seq"], action.delay_s)
        await self._deliver_sample(sample, corrupt=action.corrupt)

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


async def async_main(args: argparse.Namespace) -> int:
    client = SyntheticPodClient(args)
    return await client.run()


def cli(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(cli())
