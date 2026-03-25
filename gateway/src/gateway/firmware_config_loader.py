"""Safely load BLE constants from the pod firmware config file."""

from __future__ import annotations

import ast
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REQUIRED_CONSTANTS = (
    "POD_ID",
    "DEVICE_NAME_PREFIX",
    "SERVICE_UUID",
    "TELEMETRY_CHAR_UUID",
    "CONTROL_CHAR_UUID",
    "STATUS_CHAR_UUID",
)


@dataclass(frozen=True)
class FirmwareConfig:
    """Selected firmware constants used by the gateway."""

    config_path: Path
    pod_id: str
    device_name_prefix: str
    device_name: str
    firmware_version: str
    sample_interval_s: int
    service_uuid: str
    telemetry_char_uuid: str
    control_char_uuid: str
    status_char_uuid: str
    flag_sensor_error: int
    flag_low_batt: int

    @property
    def device_name_scan_prefix(self) -> str:
        """Prefix used for gateway scanning."""
        if self.device_name_prefix.endswith("-"):
            return self.device_name_prefix
        return f"{self.device_name_prefix}-"


def _repo_root_candidates() -> list[Path]:
    current = Path.cwd().resolve()
    module_path = Path(__file__).resolve()
    candidates: list[Path] = []
    for base in (current, *current.parents, module_path.parent, *module_path.parents):
        if base not in candidates:
            candidates.append(base)
    return candidates


def default_firmware_config_path() -> Path:
    """Locate the authoritative firmware config in the repository."""
    relative = Path("firmware") / "circuitpython-pod" / "config.py"
    for root in _repo_root_candidates():
        candidate = root / relative
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Could not locate firmware/circuitpython-pod/config.py from the current repository context."
    )


def _literal_eval(node: ast.AST, path: Path) -> Any:
    try:
        return ast.literal_eval(node)
    except (ValueError, TypeError, SyntaxError) as exc:
        raise ValueError(f"Unsupported value in firmware config {path}: {ast.dump(node)}") from exc


def _load_constant_map(path: Path) -> dict[str, Any]:
    source = path.read_text(encoding="utf-8")
    module = ast.parse(source, filename=str(path))
    values: dict[str, Any] = {}

    for statement in module.body:
        if not isinstance(statement, ast.Assign):
            continue
        if len(statement.targets) != 1 or not isinstance(statement.targets[0], ast.Name):
            continue
        name = statement.targets[0].id
        if not name.isupper():
            continue
        values[name] = _literal_eval(statement.value, path)

    return values


def _normalize_uuid(value: Any, name: str, path: Path) -> str:
    try:
        return str(uuid.UUID(str(value))).lower()
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValueError(f"{name} in {path} is not a valid UUID: {value!r}") from exc


def load_firmware_config(config_path: Path | None = None) -> FirmwareConfig:
    """Parse the firmware config without importing or executing it."""
    path = Path(config_path) if config_path else default_firmware_config_path()
    values = _load_constant_map(path)

    missing = [name for name in REQUIRED_CONSTANTS if name not in values]
    if missing:
        raise ValueError(f"Firmware config {path} is missing required constants: {', '.join(missing)}")

    pod_id = str(values["POD_ID"])
    prefix = str(values["DEVICE_NAME_PREFIX"])
    device_name = f"{prefix}-{pod_id}"

    return FirmwareConfig(
        config_path=path,
        pod_id=pod_id,
        device_name_prefix=prefix,
        device_name=device_name,
        firmware_version=str(values.get("FIRMWARE_VERSION", "")),
        sample_interval_s=int(values.get("SAMPLE_INTERVAL_S", 0)),
        service_uuid=_normalize_uuid(values["SERVICE_UUID"], "SERVICE_UUID", path),
        telemetry_char_uuid=_normalize_uuid(values["TELEMETRY_CHAR_UUID"], "TELEMETRY_CHAR_UUID", path),
        control_char_uuid=_normalize_uuid(values["CONTROL_CHAR_UUID"], "CONTROL_CHAR_UUID", path),
        status_char_uuid=_normalize_uuid(values["STATUS_CHAR_UUID"], "STATUS_CHAR_UUID", path),
        flag_sensor_error=int(values.get("FLAG_SENSOR_ERROR", 0x01)),
        flag_low_batt=int(values.get("FLAG_LOW_BATT", 0x02)),
    )
