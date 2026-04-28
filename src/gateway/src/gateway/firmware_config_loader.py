# File overview:
# - Responsibility: Safely load BLE constants from the pod firmware config file.
# - Project role: Defines configuration and top-level runtime wiring for live
#   gateway operation.
# - Main data or concerns: Runtime options, configuration values, and top-level
#   service wiring.
# - Related flow: Connects lower gateway subsystems into runnable entry points.
# - Why this matters: Top-level runtime wiring determines how the live ingestion and
#   storage path is assembled.

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
# Class purpose: Selected firmware constants used by the gateway.
# - Project role: Belongs to the gateway runtime layer and groups related state or
#   behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Top-level runtime wiring determines how the live ingestion
#   and storage path is assembled.
# - Related flow: Connects lower gateway subsystems into runnable entry points.

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
    # Method purpose: Prefix used for gateway scanning.
    # - Project role: Belongs to the gateway runtime layer and acts as a method
    #   on FirmwareConfig.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns str when the function completes successfully.
    # - Important decisions: Top-level runtime wiring determines how the live
    #   ingestion and storage path is assembled.
    # - Related flow: Connects lower gateway subsystems into runnable entry
    #   points.

    @property
    def device_name_scan_prefix(self) -> str:
        """Prefix used for gateway scanning."""
        if self.device_name_prefix.endswith("-"):
            return self.device_name_prefix
        return f"{self.device_name_prefix}-"
# Function purpose: Implements the repo root candidates step used by this subsystem.
# - Project role: Belongs to the gateway runtime layer and contributes one focused
#   step within that subsystem.
# - Inputs: No explicit arguments beyond module or instance context.
# - Outputs: Returns list[Path] when the function completes successfully.
# - Important decisions: Top-level runtime wiring determines how the live ingestion
#   and storage path is assembled.
# - Related flow: Connects lower gateway subsystems into runnable entry points.

def _repo_root_candidates() -> list[Path]:
    current = Path.cwd().resolve()
    module_path = Path(__file__).resolve()
    candidates: list[Path] = []
    for base in (current, *current.parents, module_path.parent, *module_path.parents):
        if base not in candidates:
            candidates.append(base)
    return candidates
# Function purpose: Locate the authoritative firmware config in the repository.
# - Project role: Belongs to the gateway runtime layer and contributes one focused
#   step within that subsystem.
# - Inputs: No explicit arguments beyond module or instance context.
# - Outputs: Returns Path when the function completes successfully.
# - Important decisions: Top-level runtime wiring determines how the live ingestion
#   and storage path is assembled.
# - Related flow: Connects lower gateway subsystems into runnable entry points.

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
# Function purpose: Implements the literal eval step used by this subsystem.
# - Project role: Belongs to the gateway runtime layer and contributes one focused
#   step within that subsystem.
# - Inputs: Arguments such as node, path, interpreted according to the rules encoded
#   in the body below.
# - Outputs: Returns Any when the function completes successfully.
# - Important decisions: Top-level runtime wiring determines how the live ingestion
#   and storage path is assembled.
# - Related flow: Connects lower gateway subsystems into runnable entry points.

def _literal_eval(node: ast.AST, path: Path) -> Any:
    try:
        return ast.literal_eval(node)
    except (ValueError, TypeError, SyntaxError) as exc:
        raise ValueError(f"Unsupported value in firmware config {path}: {ast.dump(node)}") from exc
# Function purpose: Loads constant map into the structure expected by downstream
#   code.
# - Project role: Belongs to the gateway runtime layer and contributes one focused
#   step within that subsystem.
# - Inputs: Arguments such as path, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns dict[str, Any] when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Connects lower gateway subsystems into runnable entry points.

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
# Function purpose: Normalizes uuid into the subsystem's stable representation.
# - Project role: Belongs to the gateway runtime layer and contributes one focused
#   step within that subsystem.
# - Inputs: Arguments such as value, name, path, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns str when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Connects lower gateway subsystems into runnable entry points.

def _normalize_uuid(value: Any, name: str, path: Path) -> str:
    try:
        return str(uuid.UUID(str(value))).lower()
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValueError(f"{name} in {path} is not a valid UUID: {value!r}") from exc
# Function purpose: Parse the firmware config without importing or executing it.
# - Project role: Belongs to the gateway runtime layer and contributes one focused
#   step within that subsystem.
# - Inputs: Arguments such as config_path, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns FirmwareConfig when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Connects lower gateway subsystems into runnable entry points.

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
