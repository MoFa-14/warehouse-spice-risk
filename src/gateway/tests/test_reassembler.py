# File overview:
# - Responsibility: Provides regression coverage for reassembler behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.protocol.json_reassembler import JsonReassembler
# Class purpose: Groups related regression checks for JsonReassembler behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class JsonReassemblerTests(unittest.TestCase):
    # Test purpose: Verifies that reassembles fragmented payload behaves as
    #   expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on JsonReassemblerTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_reassembles_fragmented_payload(self) -> None:
        reassembler = JsonReassembler()
        self.assertEqual(reassembler.feed_text('{"pod_id":"01",'), [])
        completed = reassembler.feed_text('"seq":1,"ts_uptime_s":1.0,"temp_c":20.0,"rh_pct":50.0,"flags":0}')
        self.assertEqual(
            completed,
            ['{"pod_id":"01","seq":1,"ts_uptime_s":1.0,"temp_c":20.0,"rh_pct":50.0,"flags":0}'],
        )
    # Test purpose: Verifies that handles multiple objects and string braces
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on JsonReassemblerTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_handles_multiple_objects_and_string_braces(self) -> None:
        reassembler = JsonReassembler()
        completed = reassembler.feed_text(
            '{"pod_id":"01","note":"brace { inside string","seq":1,"ts_uptime_s":1.0,"temp_c":20.0,"rh_pct":50.0,"flags":0}'
            '{"pod_id":"01","seq":2,"ts_uptime_s":2.0,"temp_c":21.0,"rh_pct":51.0,"flags":0}'
        )
        self.assertEqual(len(completed), 2)
        self.assertIn('brace { inside string', completed[0])
        self.assertIn('"seq":2', completed[1])


if __name__ == "__main__":
    unittest.main()
