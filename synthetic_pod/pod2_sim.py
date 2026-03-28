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
from sim.faults import FaultAction, FaultProfile
from sim.generator import SyntheticTelemetryGenerator


LOGGER = logging.getLogger("synthetic_pod")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synthetic pod simulator for multi-pod gateway tests")
    parser.add_argument("--gateway-host", default="127.0.0.1")
    parser.add_argument("--gateway-port", type=int, default=8765)
    parser.add_argument("--pod-id", default="02")
    parser.add_argument("--interval", type=int, default=10)
    parser.add_argument("--p-drop", type=float, default=0.0)
    parser.add_argument("--p-corrupt", type=float, default=0.0)
    parser.add_argument("--p-delay", type=float, default=0.0)
    parser.add_argument("--p-disconnect", type=float, default=0.0)
    parser.add_argument("--max-delay", type=float, default=5.0)
    parser.add_argument("--disconnect-min", type=float, default=2.0)
    parser.add_argument("--disconnect-max", type=float, default=10.0)
    parser.add_argument("--replay-buffer-size", type=int, default=300)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    if args.interval <= 0:
        parser.error("--interval must be positive.")
    if args.gateway_port <= 0:
        parser.error("--gateway-port must be positive.")
    if args.replay_buffer_size <= 0:
        parser.error("--replay-buffer-size must be positive.")
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
        self.generator = SyntheticTelemetryGenerator(pod_id=args.pod_id, interval_s=args.interval)
        self.buffer = ReplayBuffer(maxlen=args.replay_buffer_size)
        self.faults = FaultProfile(
            p_drop=args.p_drop,
            p_corrupt=args.p_corrupt,
            p_delay=args.p_delay,
            p_disconnect=args.p_disconnect,
            max_delay_s=args.max_delay,
            disconnect_min_s=args.disconnect_min,
            disconnect_max_s=args.disconnect_max,
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
            self.buffer.add(sample)
            LOGGER.info("[pod=%s] seq=%s temp=%s rh=%s", sample["pod_id"], sample["seq"], sample["temp_c"], sample["rh_pct"])
            action = self.faults.choose_action()

            if action.disconnect_s > 0:
                return action.disconnect_s
            if action.drop:
                LOGGER.info("Dropped seq=%s on purpose for fault injection.", sample["seq"])
            elif action.delay_s > 0:
                task = asyncio.create_task(self._deliver_after_delay(sample, action), name=f"delay-seq-{sample['seq']}")
                self._pending_deliveries.add(task)
                task.add_done_callback(self._pending_deliveries.discard)
            else:
                await self._deliver_sample(sample, corrupt=action.corrupt)

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
