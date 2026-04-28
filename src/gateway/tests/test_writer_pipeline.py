# File overview:
# - Responsibility: Provides regression coverage for writer pipeline behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

from __future__ import annotations

import csv
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.link.stats import LinkSnapshot
from gateway.logging.writer_pipeline import GatewayWriterPipeline, WriterDependencies
from gateway.protocol.decoder import TelemetryRecord
from gateway.storage.raw_writer import RawWriteResult
# Class purpose: Encapsulates the NoOpLinkWriter responsibilities used by this
#   module.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class _NoOpLinkWriter:
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _NoOpLinkWriter.
    # - Inputs: Arguments such as _root, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def __init__(self, _root: Path) -> None:
        self.snapshots: list[LinkSnapshot] = []
    # Method purpose: Writes snapshot into the configured destination.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _NoOpLinkWriter.
    # - Inputs: Arguments such as snapshot, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns Path when the function completes successfully.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def write_snapshot(self, snapshot: LinkSnapshot) -> Path:
        self.snapshots.append(snapshot)
        return Path("ignored.csv")
    # Method purpose: Implements the close step used by this subsystem.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _NoOpLinkWriter.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Historical fixes and future refactors both depend
    #   on this coverage staying explicit.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def close(self) -> None:
        return None
# Class purpose: Encapsulates the RecordingLegacyLogger responsibilities used by
#   this module.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class _RecordingLegacyLogger:
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _RecordingLegacyLogger.
    # - Inputs: Arguments such as _log_dir, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def __init__(self, _log_dir: Path) -> None:
        self.samples: list[int] = []
        self.link_rows = 0
    # Method purpose: Implements the log sample step used by this subsystem.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _RecordingLegacyLogger.
    # - Inputs: Arguments such as **kwargs, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Historical fixes and future refactors both depend
    #   on this coverage staying explicit.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def log_sample(self, **kwargs) -> None:
        record = kwargs["record"]
        self.samples.append(record.seq)
    # Method purpose: Implements the log link snapshot step used by this
    #   subsystem.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _RecordingLegacyLogger.
    # - Inputs: Arguments such as _snapshot, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Historical fixes and future refactors both depend
    #   on this coverage staying explicit.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def log_link_snapshot(self, _snapshot: LinkSnapshot) -> None:
        self.link_rows += 1
    # Method purpose: Implements the close step used by this subsystem.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _RecordingLegacyLogger.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Historical fixes and future refactors both depend
    #   on this coverage staying explicit.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def close(self) -> None:
        return None
# Class purpose: Encapsulates the FlakyRawWriter responsibilities used by this
#   module.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class _FlakyRawWriter:
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _FlakyRawWriter.
    # - Inputs: Arguments such as _root, state, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def __init__(self, _root: Path, state: dict[str, int]) -> None:
        self._state = state
    # Method purpose: Writes sample into the configured destination.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _FlakyRawWriter.
    # - Inputs: Arguments such as **kwargs, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns RawWriteResult when the function completes
    #   successfully.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def write_sample(self, **kwargs) -> RawWriteResult:
        self._state["attempts"] += 1
        if self._state["attempts"] == 1:
            raise IOError("simulated file lock")
        record = kwargs["record"]
        return RawWriteResult(inserted=True, duplicate=False, path=Path(f"{record.pod_id}.csv"))
    # Method purpose: Implements the close step used by this subsystem.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _FlakyRawWriter.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Historical fixes and future refactors both depend
    #   on this coverage staying explicit.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def close(self) -> None:
        return None
# Class purpose: Groups related regression checks for WriterPipeline behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class WriterPipelineTests(unittest.IsolatedAsyncioTestCase):
    # Test purpose: Verifies that writer recovers after transient io error
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on WriterPipelineTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    async def test_writer_recovers_after_transient_io_error(self) -> None:
        with TemporaryDirectory() as temp_dir:
            state = {"attempts": 0}
            legacy_instances: list[_RecordingLegacyLogger] = []

            def raw_factory(root: Path) -> _FlakyRawWriter:
                return _FlakyRawWriter(root, state)

            def legacy_factory(log_dir: Path) -> _RecordingLegacyLogger:
                logger = _RecordingLegacyLogger(log_dir)
                legacy_instances.append(logger)
                return logger

            pipeline = GatewayWriterPipeline(
                storage_root=Path(temp_dir) / "data",
                log_dir=Path(temp_dir) / "logs",
                heartbeat_interval_s=60.0,
                reopen_delay_s=0.01,
                dependencies=WriterDependencies(
                    raw_writer_factory=raw_factory,
                    link_writer_factory=_NoOpLinkWriter,
                    legacy_logger_factory=legacy_factory,
                ),
            )
            pipeline.start()

            await pipeline.enqueue_sample(
                ts_pc_utc="2026-03-27T10:00:00Z",
                record=TelemetryRecord(
                    pod_id="01",
                    seq=1,
                    ts_uptime_s=5.0,
                    temp_c=20.5,
                    rh_pct=44.0,
                    flags=0,
                ),
                rssi=-61,
                quality_flags=(),
            )
            await pipeline.stop()

            self.assertEqual(state["attempts"], 2)
            self.assertEqual(pipeline.metrics.write_errors, 1)
            self.assertEqual(pipeline.metrics.rows_written, 1)
            self.assertIsNotNone(pipeline.metrics.last_write_time_utc)
            self.assertTrue(any(logger.samples == [1] for logger in legacy_instances))
    # Test purpose: Verifies that queue pipeline writes multiple records to real
    #   CSV outputs behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on WriterPipelineTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    async def test_queue_pipeline_writes_multiple_records_to_real_csv_outputs(self) -> None:
        with TemporaryDirectory() as temp_dir:
            storage_root = Path(temp_dir) / "data"
            log_dir = Path(temp_dir) / "logs"
            pipeline = GatewayWriterPipeline(
                storage_root=storage_root,
                log_dir=log_dir,
                heartbeat_interval_s=60.0,
                reopen_delay_s=0.01,
            )
            pipeline.start()

            await pipeline.enqueue_sample(
                ts_pc_utc="2026-03-27T10:00:00Z",
                record=TelemetryRecord(
                    pod_id="01",
                    seq=1,
                    ts_uptime_s=5.0,
                    temp_c=20.0,
                    rh_pct=40.0,
                    flags=0,
                ),
                rssi=-60,
                quality_flags=(),
            )
            await pipeline.enqueue_sample(
                ts_pc_utc="2026-03-27T10:00:05Z",
                record=TelemetryRecord(
                    pod_id="01",
                    seq=2,
                    ts_uptime_s=10.0,
                    temp_c=20.3,
                    rh_pct=41.0,
                    flags=0,
                ),
                rssi=-59,
                quality_flags=(),
            )
            await pipeline.stop()

            raw_path = storage_root / "raw" / "pods" / "01" / "2026-03-27.csv"
            legacy_path = log_dir / "samples.csv"

            with raw_path.open("r", encoding="utf-8", newline="") as handle:
                raw_rows = list(csv.DictReader(handle))
            with legacy_path.open("r", encoding="utf-8", newline="") as handle:
                legacy_rows = list(csv.DictReader(handle))

            self.assertEqual(len(raw_rows), 2)
            self.assertEqual(len(legacy_rows), 2)
            self.assertEqual([row["seq"] for row in raw_rows], ["1", "2"])
            self.assertEqual([row["seq"] for row in legacy_rows], ["1", "2"])


if __name__ == "__main__":
    unittest.main()
