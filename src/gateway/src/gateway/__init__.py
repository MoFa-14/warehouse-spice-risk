# File overview:
# - Responsibility: Layer 2 BLE gateway for Warehouse Spice Risk pods.
# - Project role: Defines configuration and top-level runtime wiring for live
#   gateway operation.
# - Main data or concerns: Runtime options, configuration values, and top-level
#   service wiring.
# - Related flow: Connects lower gateway subsystems into runnable entry points.
# - Why this matters: Top-level runtime wiring determines how the live ingestion and
#   storage path is assembled.

"""Layer 2 BLE gateway for Warehouse Spice Risk pods."""

__all__ = ["__version__"]

__version__ = "0.1.0"
