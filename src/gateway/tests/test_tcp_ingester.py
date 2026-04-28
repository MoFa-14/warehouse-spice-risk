# File overview:
# - Responsibility: Provides regression coverage for TCP ingester behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

from __future__ import annotations

import asyncio
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.config import ValidationSettings
from gateway.firmware_config_loader import default_firmware_config_path, load_firmware_config
from gateway.ingesters.tcp_ingester import TcpIngester, TcpIngesterSettings
from gateway.multi.record import TelemetryRecord
from gateway.multi.router import PodRouter
# Class purpose: Groups related regression checks for TcpIngester behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class TcpIngesterTests(unittest.IsolatedAsyncioTestCase):
    # Test purpose: Verifies that TCP ingester requests resend after corrupt
    #   line behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on TcpIngesterTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    async def test_tcp_ingester_requests_resend_after_corrupt_line(self) -> None:
        with TemporaryDirectory() as temp_dir:
            queue: asyncio.Queue[TelemetryRecord] = asyncio.Queue()
            router = PodRouter(
                queue=queue,
                firmware=load_firmware_config(default_firmware_config_path()),
                validation=ValidationSettings(temp_min_c=-20.0, temp_max_c=80.0),
                data_root=Path(temp_dir) / "data",
            )
            ingester = TcpIngester(
                queue=queue,
                router=router,
                settings=TcpIngesterSettings(host="127.0.0.1", port=0),
            )
            router.start()
            await ingester.start()

            port = ingester._server.sockets[0].getsockname()[1]
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b'{"pod_id":"02","seq":1,"ts_uptime_s":10.0,"temp_c":24.1,"rh_pct":44.2,"flags":0}\n')
            await writer.drain()
            await asyncio.sleep(0.1)

            writer.write(b'{"pod_id":"02","seq"')
            writer.write(b"\n")
            await writer.drain()

            response = await asyncio.wait_for(reader.readline(), timeout=1.0)
            command = json.loads(response.decode("utf-8"))

            writer.close()
            await writer.wait_closed()
            await ingester.stop()
            await router.stop()

            self.assertEqual(command["cmd"], "REQ_SEQ")
            self.assertEqual(command["pod_id"], "02")
            self.assertEqual(command["seq"], 2)


if __name__ == "__main__":
    unittest.main()
