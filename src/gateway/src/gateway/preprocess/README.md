# Gateway Preprocessing Utilities

This folder contains the utilities that convert stored raw telemetry into
cleaned, resampled, and exportable analysis forms.

## Purpose

The forecasting loop mainly reads directly from stored telemetry through the
forecast storage adapter, but the project also needs preprocessing utilities
for:

- processed CSV outputs,
- training dataset preparation,
- day-level resampling,
- dew-point enrichment,
- and offline historical analysis.

This folder exists to keep those transformations separate from the live
ingestion code.

## Files

### `clean.py`

Contains:

- `RawSampleRow`
- `CleanSampleRow`
- `read_raw_samples`
- `clean_samples`

This file turns raw stored rows into cleaned forms that can be resampled or
exported. It exists because raw telemetry may contain missing values or
irregularities that should be handled consistently before day-level processed
artefacts are generated.

### `resample.py`

Contains:

- `ProcessedRow`
- `resample_day`
- interpolation helpers for small gaps

This file is responsible for converting day-level raw history into a regular
grid. The same general idea underlies the forecasting system's insistence on a
stable time grid, even though the forecasting loop uses its own history adapter.

### `dewpoint.py`

Contains `dew_point_c`, which derives dew point from temperature and relative
humidity. The repository uses dew-point derivation rather than a separately
measured dew-point sensor or a separate forecast model target.

### `export.py`

Contains:

- `preprocess_day_file`
- `preprocess_date_range`
- `export_training_dataset`

This file coordinates the preprocessing pipeline for offline dataset and CSV
production tasks.

## Design Notes

- Preprocessing is treated as a reproducible pipeline rather than ad hoc
  notebook logic.
- Dew point is derived consistently here as well as elsewhere in the project so
  processed data aligns with dashboard and forecasting assumptions.

## Limitations

- These utilities are highly useful for historical analysis, but they are not
  the authoritative live forecasting path.
- A reader should distinguish between day-level processed exports and the live
  minute-window forecasting workflow.
