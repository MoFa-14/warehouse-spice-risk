# Warehouse Microclimate Model

This simulator models pod 02 as a warehouse micro-zone rather than as a generic random sensor feed. The design is intentionally lightweight, but it maps to the warehouse variability concepts discussed in:

- Carlo Maria Aloe and Annarita De Maio, "Balancing Temperature and Humidity Control in Storage Location Assignment: An Optimization Perspective in Refrigerated Warehouses", *Sustainability* 2025, 17(16), 7477. https://doi.org/10.3390/su17167477

## Why This Is A Reasonable Imitation

The paper emphasizes that refrigerated warehouses are not environmentally uniform. Instead, they show:

- spatial temperature stratification
- environmental variability across different locations
- fluctuations caused by door openings and operational activity
- value from real-time sensor mapping across the warehouse

Pod 02 now imitates those ideas with a small zone-based generator that produces different but plausible local conditions over time.

## Paper Concept To Simulator Mapping

- Zone baselines:
  `base_temp_c` and `base_rh_pct` represent spatial variability. Different zones start from different nominal conditions instead of sharing one warehouse-wide average.

- Bristol-inspired seasonal forcing:
  A lightweight reference curve nudges the indoor target so summer settles warmer than winter and daytime settles warmer than night. This is a trend guide, not a replay of outdoor weather.

- Stratified profile:
  `upper_rack_stratified` encodes a warmer and slightly drier baseline than the interior profile. This is a simplified representation of vertical stratification rather than a full physical airflow model.

- Door-opening disturbances:
  Disturbance events move temperature and humidity toward the outdoor reference instead of always pushing upward. This gives more natural entrance behavior because a cool damp day can produce a slight cooling event, while a warm period can produce a mild warming event.

- Exponential recovery:
  After a disturbance, the simulator decays back toward baseline using a configurable recovery constant. This approximates how local conditions settle after a door event or handling burst.

- Active-hours schedule:
  Event rates are higher during active hours and lower outside them, representing the paper's practical idea that operational activity changes warehouse environmental stress over time.

- Mean-reverting indoor drift:
  Instead of an unrestricted random walk, the baseline now drifts toward an indoor target derived from season and hour-of-day. This keeps the slope flatter and more realistic while still allowing continuous movement.

- Drift and short-term noise:
  Bounded stochastic drift plus short-term noise represent the continuous background variability captured by real sensor mapping, without pretending to be a full thermodynamic model.

- Bursty communication faults:
  Optional burst loss/delay lets radio issues cluster during disturbances, which is useful for gateway stress testing when environmental activity and communication quality degrade together.

## Zone Profiles

- `interior_stable`:
  Interior aisle behavior with low variance, low disturbance rate, and slow recovery.

- `entrance_disturbed`:
  Entrance-facing behavior with higher variance, higher humidity baseline, stronger spikes, and higher event rate during active hours.

- `upper_rack_stratified`:
  Upper-rack behavior with a warmer and slightly drier baseline plus moderate variance, representing a vertically distinct warehouse layer.

## Parameter Rationale

- Temperature is clipped to `[-5, 45] C` and RH to `[0, 100]%` so the synthetic pod always stays within plausible operating bounds.
- Event rates are defined per hour because disturbances are not expected every sample.
- Recovery uses `exp(-dt / tau)` to make disturbance effects smooth and physically plausible enough for system testing.
- The Bristol reference is attenuated indoors through seasonal and diurnal weights so warehouse readings stay smoother than outside conditions.
- Drift is bounded separately from hard output clipping so the zone can wander realistically without becoming unstable.

## Intended Use

This model is meant for:

- proving that the gateway can ingest concurrent pods from different warehouse-like zones
- creating visibly distinct pod 02 behavior by zone profile
- stress testing resend, replay, and storage logic under realistic environmental variation

It is not intended as a physics-accurate CFD or refrigeration model.
