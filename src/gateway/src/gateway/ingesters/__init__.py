# File overview:
# - Responsibility: Gateway ingestion adapters for BLE and TCP telemetry.
# - Project role: Accepts live telemetry from transport-specific sources and
#   converts it into normalized gateway records.
# - Main data or concerns: Raw transport messages, decoded telemetry payloads, and
#   connection events.
# - Related flow: Receives BLE or TCP input and passes decoded records into routing
#   and storage.
# - Why this matters: All later persistence and forecasting logic depends on
#   ingestion normalizing the live inputs correctly.

"""Gateway ingestion adapters for BLE and TCP telemetry."""

from gateway.ingesters.ble_ingester import BleIngester
from gateway.ingesters.tcp_ingester import TcpIngester

__all__ = [
    "BleIngester",
    "TcpIngester",
]
