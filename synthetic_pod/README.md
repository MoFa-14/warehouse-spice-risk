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
  More variable entrance-facing behavior with stronger disturbance spikes and more active-hours events.

- `upper_rack_stratified`:
  A warmer, slightly drier upper-rack profile to imitate vertical stratification.

These profiles provide explicit spatial variability so pod 02 can represent a warehouse location that behaves differently from pod 01.

### Disturbance events and recovery

The generator uses occasional disturbance events to imitate door openings or handling bursts:

- temperature and RH jump upward during an event
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
- slow bounded random walk adds gradual change over time
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
python .\synthetic_pod\pod2_sim.py --gateway-port 8765 --interval 60 --zone-profile entrance_disturbed --event-rate-active-hours 1.8 --event-spike-rh 10 --verbose
```

Upper-rack stratified zone:

```powershell
python .\synthetic_pod\pod2_sim.py --gateway-port 8765 --interval 60 --zone-profile upper_rack_stratified --base-temp 24.5 --base-rh 33 --verbose
```

Entrance zone with bursty loss/delay:

```powershell
python .\synthetic_pod\pod2_sim.py --gateway-port 8765 --interval 60 --zone-profile entrance_disturbed --p-drop 0.05 --p-delay 0.10 --burst-loss on --burst-duration-seconds 40 --burst-multiplier 3 --verbose
```

## Paper Alignment

The simulator rationale and feature-to-paper mapping are documented in:

- `synthetic_pod/docs/warehouse_microclimate_model.md`
