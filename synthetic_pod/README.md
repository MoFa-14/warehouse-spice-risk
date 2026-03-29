# Synthetic Pod 02

This simulator behaves like a second pod for gateway stress testing without requiring extra hardware.

Run it from the repository root:

```powershell
python .\synthetic_pod\pod2_sim.py --gateway-host 127.0.0.1 --gateway-port 8765 --pod-id 02 --interval 60 --zone-profile entrance_disturbed --verbose
```

Helper script:

```powershell
.\scripts\run_pod2.ps1 -GatewayPort 8765 -ZoneProfile entrance_disturbed
```

Useful fault-injection options:

- `--p-drop 0.1`
- `--p-corrupt 0.05`
- `--p-delay 0.2`
- `--p-disconnect 0.02`
- `--replay-buffer-size 300`
- `--burst-loss on --burst-duration-seconds 40 --burst-multiplier 3`

Useful microclimate options:

- `--zone-profile {interior_stable, entrance_disturbed, upper_rack_stratified}`
- `--timezone Europe/London`
- `--start-local 2026-07-15T09:00:00`
- `--base-temp`
- `--base-rh`
- `--noise-temp`
- `--noise-rh`
- `--drift-temp`
- `--drift-rh`
- `--event-rate`
- `--event-rate-active-hours`
- `--event-spike-temp`
- `--event-spike-rh`
- `--recovery-tau-seconds`

The simulator keeps a replay buffer so the gateway can request:

- `REQ_SEQ`
- `REQ_FROM_SEQ`

When a resend command arrives, the simulator replays the requested samples immediately in order.

## Warehouse Realism Model (Microclimates)

Pod 02 now acts like a configurable warehouse micro-zone instead of producing a generic random feed.

### Zone profiles

- `interior_stable`:
  Low-variance interior aisle behavior with minor drift and rare disturbances.

- `entrance_disturbed`:
  Entrance-facing behavior with visible but gentler operational disturbance, tuned so pod 02 looks more natural and less jagged.

- `upper_rack_stratified`:
  A warmer, slightly drier upper-rack profile to imitate vertical stratification.

These profiles provide explicit spatial variability so pod 02 can represent a warehouse location that behaves differently from pod 01.

### Bristol-inspired seasonal and day-night trend

Pod 02 now follows a damped indoor climate target instead of drifting around a fixed flat baseline:

- the target is shaped by a lightweight Bristol-like outdoor reference
- summer targets settle warmer than winter targets
- daytime targets rise above overnight targets
- the warehouse signal is smoother and less extreme than outdoors

The simulator does not copy live outdoor readings directly. It uses those trends as a guide, then attenuates them to fit a closed warehouse with slower thermal change and smaller RH swings.

Use `--start-local` if you want to anchor a demo in a specific season or time of day. That same anchor now also aligns the active-hours schedule, so daytime disturbance rates and noise levels match the chosen local clock.

If your Windows Python build does not include the IANA timezone database, `--timezone Europe/London` still works in fallback mode for the simulator's internal local clock instead of failing at startup.

### Disturbance events and recovery

The generator uses occasional disturbance events to imitate door openings or handling bursts:

- event direction now follows the indoor/outdoor gap, so an opening can gently cool or warm the zone instead of always spiking upward
- RH disturbances similarly move toward the outdoor reference instead of using one fixed sign
- the signal then decays back toward baseline using exponential recovery

Pod 02 prints whether each sample is currently in a disturbance state.

### Multi-period activity schedule

The simulator uses uptime modulo 24 hours:

- active hours default to `08:00-18:00`
- disturbance frequency is higher during active hours
- off-hours noise is lower

This gives pod 02 a simple operational-day rhythm without requiring real calendar integration.

### Drift and noise

- short-term Gaussian noise adds local variability
- a mean-reverting drift follows the Bristol-inspired warehouse target instead of wandering freely
- hard clipping keeps temperature and RH plausible

### Bursty communication faults

The existing drop/delay/corrupt/disconnect fault model is still available. If `--burst-loss on` is used, loss and delay can temporarily intensify during disturbances so communication faults cluster in short windows instead of appearing only as isolated independent events.

## Example Commands

Stable interior zone:

```powershell
python .\synthetic_pod\pod2_sim.py --gateway-port 8765 --interval 60 --zone-profile interior_stable --verbose
```

Entrance zone with stronger disturbances:

```powershell
python .\synthetic_pod\pod2_sim.py --gateway-port 8765 --interval 60 --zone-profile entrance_disturbed --verbose
```

Upper-rack stratified zone:

```powershell
python .\synthetic_pod\pod2_sim.py --gateway-port 8765 --interval 60 --zone-profile upper_rack_stratified --base-temp 24.5 --base-rh 33 --verbose
```

Entrance zone with bursty loss/delay:

```powershell
python .\synthetic_pod\pod2_sim.py --gateway-port 8765 --interval 60 --zone-profile entrance_disturbed --p-drop 0.05 --p-delay 0.10 --burst-loss on --burst-duration-seconds 40 --burst-multiplier 3 --verbose
```

Summer-aligned demo:

```powershell
python .\synthetic_pod\pod2_sim.py --gateway-port 8765 --interval 60 --zone-profile entrance_disturbed --timezone Europe/London --start-local 2026-07-15T09:00:00 --verbose
```

## Paper Alignment

The simulator rationale and feature-to-paper mapping are documented in:

- `synthetic_pod/docs/warehouse_microclimate_model.md`
