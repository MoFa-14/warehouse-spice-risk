# Dashboard Layer 4 (Presentation Only)

This package implements the Flask dashboard and presentation layer for Warehouse Spice Risk.

Scope limits for this phase:

- Reads live telemetry from SQLite, with processed CSVs used when available
- Uses rule-based alerts only
- Does not include machine learning, forecasting, anomaly detection, or model training
- The Prediction page is intentionally a placeholder

## Expected Data Layout

The dashboard reads data from the repository `data/` folder:

- Primary live source: `data/db/telemetry.sqlite`
- `data/raw/pods/<pod_id>/YYYY-MM-DD.csv`
- `data/raw/link_quality/YYYY-MM-DD.csv` (optional)
- `data/processed/pods/<pod_id>/YYYY-MM-DD_processed.csv` (optional)

The dashboard now prefers live SQLite data for raw telemetry and link quality. Processed CSV files are still used when available for cleaned historical views. If the SQLite database is missing, the dashboard falls back to the raw/link CSV files.

## Install

From the repository root:

```powershell
.\.venv\Scripts\python.exe -m pip install -r .\dashboard\requirements.txt
```

## Run

From the repository root:

```powershell
.\scripts\run_dashboard.ps1
```

Or run it manually from the dashboard directory:

```powershell
cd .\dashboard
..\.venv\Scripts\python.exe -m flask --app app.main run --debug
```

Open `http://127.0.0.1:5000` in your browser.

Once this SQLite-aware code is running, you do not need to restart Flask for each new sensor reading. The dashboard queries the database on each request, and the UI auto-refreshes every 5 seconds by default. You only need to restart the dashboard once after changing the dashboard code itself.

## Pages

- `/` overview with latest readings per pod
- `/pods/<pod_id>` pod detail with historical charts
- `/health` link quality and data quality summary
- `/alerts` active and acknowledged rule-based alerts
- `/prediction` placeholder only: `Not implemented yet`

## Alert Levels

The dashboard uses these rule-based alert levels:

- Level 0 `OPTIMAL`: temperature and humidity within ideal storage range
- Level 1 `GOOD/ACCEPTABLE`: safe but slightly outside optimal
- Level 2 `WARNING`: likely to harm quality if sustained
- Level 3 `HIGH RISK`: strong risk of caking and quality loss
- Level 4 `CRITICAL`: rapid spoilage or mold risk

Threshold constants implemented in `dashboard/app/services/thresholds.py`:

- Temperature: optimal `10-21 C`, warning above `22 C`, critical at `25 C`
- Relative humidity: low warning below `30%`, ideal `30-50%`, warning above `50%`, high risk above `60%`, critical at `65%`

If both temperature and humidity rules apply, the dashboard takes the maximum severity.

## Acknowledgements

Alert acknowledgements are stored locally in:

- `dashboard/app/runtime/acks.json`

This is a lightweight UI-only mute for 30 minutes by default. It does not affect CSV logging.

## Notes

- Alerts are rule-based only in this phase.
- Dew point is displayed from live SQLite raw readings by computing it from temperature and humidity, with processed CSVs still used as an optional fallback.
- If the newest sample is incomplete, the dashboard shows the latest sample timestamp and also shows the last complete measurement timestamp for context.
- Dew point does not generate alerts yet.
- Prediction is placeholder-only and no ML code is included.
