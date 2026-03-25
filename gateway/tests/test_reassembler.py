from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.protocol.json_reassembler import JsonReassembler


class JsonReassemblerTests(unittest.TestCase):
    def test_reassembles_fragmented_payload(self) -> None:
        reassembler = JsonReassembler()
        self.assertEqual(reassembler.feed_text('{"pod_id":"01",'), [])
        completed = reassembler.feed_text('"seq":1,"ts_uptime_s":1.0,"temp_c":20.0,"rh_pct":50.0,"flags":0}')
        self.assertEqual(
            completed,
            ['{"pod_id":"01","seq":1,"ts_uptime_s":1.0,"temp_c":20.0,"rh_pct":50.0,"flags":0}'],
        )

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
