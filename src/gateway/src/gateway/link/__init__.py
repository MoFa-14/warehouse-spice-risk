# File overview:
# - Responsibility: Link quality tracking helpers.
# - Project role: Computes communication quality, sequence gaps, and timing
#   diagnostics.
# - Main data or concerns: Sequence counters, timestamps, connectivity statistics,
#   and missing-rate metrics.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.
# - Why this matters: Link-quality interpretation matters because missing data
#   changes how later telemetry should be trusted.

"""Link quality tracking helpers."""
