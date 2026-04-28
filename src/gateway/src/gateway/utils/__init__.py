# File overview:
# - Responsibility: Utility helpers for the gateway package.
# - Project role: Provides reusable low-level helpers for timing, retry logic, and
#   sequence handling.
# - Main data or concerns: Helper arguments, timestamps, counters, and shared return
#   values.
# - Related flow: Supports higher-level gateway modules with focused helper
#   behavior.
# - Why this matters: Keeping small utility rules centralized prevents subtle
#   duplication across transport and storage code.

"""Utility helpers for the gateway package."""
