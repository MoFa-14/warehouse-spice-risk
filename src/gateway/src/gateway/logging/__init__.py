# File overview:
# - Responsibility: Gateway logging package.
# - Project role: Coordinates append-only file writing, locking, and
#   persistence-side buffering.
# - Main data or concerns: CSV rows, write queues, locks, and storage paths.
# - Related flow: Receives normalized records from routing or preprocessing and
#   passes persisted outputs to later analysis.
# - Why this matters: Centralizing write behavior avoids duplicate storage-side
#   assumptions across the gateway.

"""Gateway logging package.

Keep package initialization minimal so importing one logging submodule does not
eagerly import the others and accidentally pull in storage modules too early.
"""

__all__: list[str] = []
