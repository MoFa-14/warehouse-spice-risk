from __future__ import annotations

import asyncio
import csv
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.config import ValidationSettings
from gateway.control.resend import ResendController
from gateway.firmware_config_loader import default_firmware_config_path, load_firmware_config
from gateway.multi.record import TelemetryRecord
from gateway.multi.router import PodRouter, PodStats


class _DuplicateThenAdvanceWriter:
    def __init__(self) -> None:
        self.records: list[int] = []

    def write_record(self, record: TelemetryRecord, *, quality_flags) -> object:
        self.records.append(record.seq)
        if record.seq == 1:
            return type("Result", (), {"inserted": True, "duplicate": False})()
        return type("Result", (), {"inserted": False, "duplicate": True})()

    def write_link_snapshot(self, snapshot) -> None:
        return None

    def close(self) -> None:
        return None


class _RecordingResendController:
    def __init__(self) -> None:
        self.seq_requests: list[tuple[str, int]] = []
        self.from_seq_requests: list[tuple[str, int]] = []

    async def request_seq(self, pod_id: str, seq: int) -> None:
        self.seq_requests.append((pod_id, seq))

    async def request_from_seq(self, pod_id: str, from_seq: int) -> None:
        self.from_seq_requests.append((pod_id, from_seq))


class MultiRouterTests(unittest.IsolatedAsyncioTestCase):
    async def test_router_writes_per_pod_files_and_requests_resend_on_gap(self) -> None:
        with TemporaryDirectory() as temp_dir:
            queue: asyncio.Queue[TelemetryRecord] = asyncio.Queue()
            router = PodRouter(
                queue=queue,
                firmware=load_firmware_config(default_firmware_config_path()),
                validation=ValidationSettings(temp_min_c=-20.0, temp_max_c=80.0),
                data_root=Path(temp_dir) / "data",
            )
            controller = _RecordingResendController()
            router.register_resend_controller("02", controller)
            router.start()

            await queue.put(
                TelemetryRecord(
                    pod_id="01",
                    seq=1,
                    ts_uptime_s=10.0,
                    temp_c=23.1,
                    rh_pct=40.0,
                    flags=0,
                    rssi=-61,
                    source="BLE",
                    ts_pc_utc="2026-03-27T12:00:00Z",
                )
            )
            await queue.put(
                TelemetryRecord(
                    pod_id="02",
                    seq=1,
                    ts_uptime_s=10.0,
                    temp_c=24.0,
                    rh_pct=45.0,
                    flags=0,
                    rssi=None,
                    source="TCP",
                    ts_pc_utc="2026-03-27T12:00:05Z",
                )
            )
            await queue.put(
                TelemetryRecord(
                    pod_id="02",
                    seq=3,
                    ts_uptime_s=30.0,
                    temp_c=24.2,
                    rh_pct=45.5,
                    flags=0,
                    rssi=None,
                    source="TCP",
                    ts_pc_utc="2026-03-27T12:00:25Z",
                )
            )

            await queue.join()
            router.note_connected("01", "BLE", last_rssi=-61)
            router.note_connected("02", "TCP")
            router.note_disconnected("02", "TCP")
            for snapshot in router.build_link_snapshots(ts_pc_utc="2026-03-27T12:00:30Z"):
                router.write_link_snapshot(snapshot)
            snapshots = router.stats_snapshot()
            await router.stop()

            pod1_path = Path(temp_dir) / "data" / "raw" / "pods" / "01" / "2026-03-27.csv"
            pod2_path = Path(temp_dir) / "data" / "raw" / "pods" / "02" / "2026-03-27.csv"
            legacy_samples_path = Path(temp_dir) / "gateway" / "logs" / "samples.csv"
            canonical_link_path = Path(temp_dir) / "data" / "raw" / "link_quality" / "2026-03-27.csv"
            legacy_link_path = Path(temp_dir) / "gateway" / "logs" / "link_quality.csv"

            with pod1_path.open("r", encoding="utf-8", newline="") as handle:
                pod1_rows = list(csv.DictReader(handle))
            with pod2_path.open("r", encoding="utf-8", newline="") as handle:
                pod2_rows = list(csv.DictReader(handle))
            with legacy_samples_path.open("r", encoding="utf-8", newline="") as handle:
                legacy_rows = list(csv.DictReader(handle))
            with canonical_link_path.open("r", encoding="utf-8", newline="") as handle:
                canonical_link_rows = list(csv.DictReader(handle))
            with legacy_link_path.open("r", encoding="utf-8", newline="") as handle:
                legacy_link_rows = list(csv.DictReader(handle))

            self.assertEqual(len(pod1_rows), 1)
            self.assertEqual(len(pod2_rows), 2)
            self.assertEqual(len(legacy_rows), 3)
            self.assertEqual(len(canonical_link_rows), 2)
            self.assertEqual(len(legacy_link_rows), 2)
            self.assertEqual({row["pod_id"] for row in legacy_rows}, {"01", "02"})
            self.assertEqual(controller.from_seq_requests, [("02", 2)])
            self.assertEqual({snapshot.pod_id for snapshot in snapshots}, {"01", "02"})

    async def test_router_throttles_repeat_resend_requests_after_duplicate_progress(self) -> None:
        queue: asyncio.Queue[TelemetryRecord] = asyncio.Queue()
        router = PodRouter(
            queue=queue,
            firmware=load_firmware_config(default_firmware_config_path()),
            validation=ValidationSettings(temp_min_c=-20.0, temp_max_c=80.0),
            data_root=Path("."),
            resend_cooldown_s=60.0,
            duplicate_log_interval_s=60.0,
        )
        router.writer.close()
        router.writer = _DuplicateThenAdvanceWriter()
        controller = _RecordingResendController()
        router.register_resend_controller("02", controller)
        router.start()

        await queue.put(
            TelemetryRecord(
                pod_id="02",
                seq=1,
                ts_uptime_s=10.0,
                temp_c=24.0,
                rh_pct=45.0,
                flags=0,
                rssi=None,
                source="TCP",
                ts_pc_utc="2026-03-27T12:00:00Z",
            )
        )
        await queue.put(
            TelemetryRecord(
                pod_id="02",
                seq=3,
                ts_uptime_s=30.0,
                temp_c=24.1,
                rh_pct=45.1,
                flags=0,
                rssi=None,
                source="TCP",
                ts_pc_utc="2026-03-27T12:00:20Z",
            )
        )
        await queue.put(
            TelemetryRecord(
                pod_id="02",
                seq=4,
                ts_uptime_s=40.0,
                temp_c=24.2,
                rh_pct=45.2,
                flags=0,
                rssi=None,
                source="TCP",
                ts_pc_utc="2026-03-27T12:00:30Z",
            )
        )

        await queue.join()
        await router.stop()

        self.assertEqual(controller.from_seq_requests, [("02", 2)])

    async def test_router_detects_soft_reload_sequence_drop_with_higher_uptime(self) -> None:
        stats = PodStats(
            pod_id="01",
            source="BLE",
            last_seq_high_water=104,
            last_uptime_s=8207.6,
        )
        record = TelemetryRecord(
            pod_id="01",
            seq=89,
            ts_uptime_s=11640.7,
            temp_c=18.19,
            rh_pct=32.31,
            flags=0,
            rssi=-43,
            source="BLE",
            ts_pc_utc="2026-03-28T18:30:36Z",
        )

        self.assertTrue(PodRouter._should_reset_sequence(stats, record))


if __name__ == "__main__":
    unittest.main()
