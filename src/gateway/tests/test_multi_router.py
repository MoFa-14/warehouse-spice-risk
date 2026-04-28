# File overview:
# - Responsibility: Provides regression coverage for multi router behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

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
# Class purpose: Encapsulates the DuplicateThenAdvanceWriter responsibilities used
#   by this module.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class _DuplicateThenAdvanceWriter:
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _DuplicateThenAdvanceWriter.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def __init__(self) -> None:
        self.records: list[int] = []
    # Method purpose: Writes record into the configured destination.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _DuplicateThenAdvanceWriter.
    # - Inputs: Arguments such as record, quality_flags, interpreted according
    #   to the rules encoded in the body below.
    # - Outputs: Returns object when the function completes successfully.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def write_record(self, record: TelemetryRecord, *, quality_flags) -> object:
        self.records.append(record.seq)
        if record.seq == 1:
            return type("Result", (), {"inserted": True, "duplicate": False})()
        return type("Result", (), {"inserted": False, "duplicate": True})()
    # Method purpose: Writes link snapshot into the configured destination.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _DuplicateThenAdvanceWriter.
    # - Inputs: Arguments such as snapshot, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def write_link_snapshot(self, snapshot) -> None:
        return None
    # Method purpose: Implements the close step used by this subsystem.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _DuplicateThenAdvanceWriter.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Historical fixes and future refactors both depend
    #   on this coverage staying explicit.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def close(self) -> None:
        return None
# Class purpose: Encapsulates the RecordingResendController responsibilities used by
#   this module.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class _RecordingResendController:
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _RecordingResendController.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def __init__(self) -> None:
        self.seq_requests: list[tuple[str, int]] = []
        self.from_seq_requests: list[tuple[str, int]] = []
    # Method purpose: Implements the request seq step used by this subsystem.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _RecordingResendController.
    # - Inputs: Arguments such as pod_id, seq, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Historical fixes and future refactors both depend
    #   on this coverage staying explicit.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    async def request_seq(self, pod_id: str, seq: int) -> None:
        self.seq_requests.append((pod_id, seq))
    # Method purpose: Implements the request from seq step used by this
    #   subsystem.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _RecordingResendController.
    # - Inputs: Arguments such as pod_id, from_seq, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Historical fixes and future refactors both depend
    #   on this coverage staying explicit.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    async def request_from_seq(self, pod_id: str, from_seq: int) -> None:
        self.from_seq_requests.append((pod_id, from_seq))
# Class purpose: Encapsulates the RecordingSqliteLikeWriter responsibilities used by
#   this module.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class _RecordingSqliteLikeWriter:
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _RecordingSqliteLikeWriter.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def __init__(self) -> None:
        self.records: list[tuple[int, tuple[str, ...]]] = []
        self.events: list[tuple[str, str, str]] = []
    # Method purpose: Writes record into the configured destination.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _RecordingSqliteLikeWriter.
    # - Inputs: Arguments such as record, quality_flags, interpreted according
    #   to the rules encoded in the body below.
    # - Outputs: Returns object when the function completes successfully.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def write_record(self, record: TelemetryRecord, *, quality_flags) -> object:
        self.records.append((record.seq, tuple(quality_flags)))
        return type("Result", (), {"inserted": True, "duplicate": False})()
    # Method purpose: Writes link snapshot into the configured destination.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _RecordingSqliteLikeWriter.
    # - Inputs: Arguments such as _snapshot, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def write_link_snapshot(self, _snapshot) -> None:
        return None
    # Method purpose: Implements the log event step used by this subsystem.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _RecordingSqliteLikeWriter.
    # - Inputs: Arguments such as ts_pc_utc, level, pod_id, message, interpreted
    #   according to the rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Historical fixes and future refactors both depend
    #   on this coverage staying explicit.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def log_event(self, *, ts_pc_utc: str, level: str, pod_id: str | None = None, message: str) -> None:
        self.events.append((level, str(pod_id or ""), message))
    # Method purpose: Implements the close step used by this subsystem.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _RecordingSqliteLikeWriter.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Historical fixes and future refactors both depend
    #   on this coverage staying explicit.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def close(self) -> None:
        return None
# Class purpose: Groups related regression checks for MultiRouter behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class MultiRouterTests(unittest.IsolatedAsyncioTestCase):
    # Test purpose: Verifies that router writes per pod files and requests
    #   resend on gap behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on MultiRouterTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

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
    # Test purpose: Verifies that router throttles repeat resend requests after
    #   duplicate progress behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on MultiRouterTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

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
    # Test purpose: Verifies that router detects soft reload sequence drop with
    #   higher uptime behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on MultiRouterTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

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
    # Test purpose: Verifies that router detects small sequence restart when
    #   uptime keeps advancing behaves as expected under this regression
    #   scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on MultiRouterTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    async def test_router_detects_small_sequence_restart_when_uptime_keeps_advancing(self) -> None:
        stats = PodStats(
            pod_id="01",
            source="BLE",
            last_seq_high_water=4,
            last_uptime_s=12846.4,
        )
        record = TelemetryRecord(
            pod_id="01",
            seq=2,
            ts_uptime_s=12864.0,
            temp_c=18.48,
            rh_pct=33.32,
            flags=0,
            rssi=-43,
            source="BLE",
            ts_pc_utc="2026-03-28T19:06:20Z",
        )

        self.assertTrue(PodRouter._should_reset_sequence(stats, record))
    # Test purpose: Verifies that router flags time sync anomaly and logs
    #   gateway events behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on MultiRouterTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    async def test_router_flags_time_sync_anomaly_and_logs_gateway_events(self) -> None:
        queue: asyncio.Queue[TelemetryRecord] = asyncio.Queue()
        router = PodRouter(
            queue=queue,
            firmware=load_firmware_config(default_firmware_config_path()),
            validation=ValidationSettings(temp_min_c=-20.0, temp_max_c=80.0),
            data_root=Path("."),
        )
        router.writer.close()
        router.writer = _RecordingSqliteLikeWriter()
        controller = _RecordingResendController()
        router.register_resend_controller("01", controller)
        router.start()

        await queue.put(
            TelemetryRecord(
                pod_id="01",
                seq=1,
                ts_uptime_s=10.0,
                temp_c=20.0,
                rh_pct=45.0,
                flags=0,
                rssi=-50,
                source="BLE",
                ts_pc_utc="2026-03-28T12:00:00Z",
            )
        )
        await queue.put(
            TelemetryRecord(
                pod_id="01",
                seq=3,
                ts_uptime_s=70.0,
                temp_c=20.5,
                rh_pct=45.5,
                flags=0,
                rssi=-49,
                source="BLE",
                ts_pc_utc="2026-03-28T12:05:00Z",
            )
        )

        await queue.join()
        writer = router.writer
        await router.stop()

        self.assertEqual(controller.from_seq_requests, [("01", 2)])
        self.assertIn("time_sync_anomaly", writer.records[-1][1])
        self.assertTrue(any(message.startswith("resend_request") for _, _, message in writer.events))
        self.assertTrue(any(message.startswith("time_sync_anomaly") for _, _, message in writer.events))


if __name__ == "__main__":
    unittest.main()
