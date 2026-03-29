"""Gateway forecasting integration helpers."""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_forecasting_package() -> None:
    current = Path(__file__).resolve()
    for base in (current.parent, *current.parents):
        candidate = base / "ml" / "src"
        if (candidate / "forecasting").exists():
            candidate_text = str(candidate)
            if candidate_text not in sys.path:
                sys.path.insert(0, candidate_text)
            return


_ensure_forecasting_package()
