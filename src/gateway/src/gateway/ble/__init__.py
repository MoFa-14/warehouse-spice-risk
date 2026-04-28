# File overview:
# - Responsibility: BLE helpers for scanning, GATT access, and session management.
# - Project role: Handles BLE discovery, connection, and GATT interaction for the
#   physical pod path.
# - Main data or concerns: BLE addresses, characteristics, notifications, and
#   connection state.
# - Related flow: Receives BLE-facing configuration or requests and passes transport
#   results to ingestion.
# - Why this matters: The physical pod path depends on this layer to convert radio
#   interaction into usable gateway events.

"""BLE helpers for scanning, GATT access, and session management."""
