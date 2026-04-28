# File overview:
# - Responsibility: Dashboard package initializer.
# - Project role: Defines app configuration, initialization, route setup, and
#   dashboard-wide utilities.
# - Main data or concerns: Configuration values, route parameters, and app-level
#   helper state.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.
# - Why this matters: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.

"""Dashboard package initializer."""

from app.main import create_app

__all__ = ["create_app"]
