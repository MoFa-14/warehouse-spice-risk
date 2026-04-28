# File overview:
# - Responsibility: CLI entrypoints for storage and multi-pod gateway tasks.
# - Project role: Exposes operator-facing commands for gateway runtime, storage, and
#   forecasting tasks.
# - Main data or concerns: CLI arguments, runtime options, and command outcomes.
# - Related flow: Receives command-line arguments and dispatches to gateway
#   services.
# - Why this matters: Operational workflows remain reproducible when the CLI
#   documents the same runtime paths used elsewhere.

"""CLI entrypoints for storage and multi-pod gateway tasks."""
