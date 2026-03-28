# Synthetic Pod 02

This simulator behaves like a second pod for gateway stress testing without requiring extra hardware.

Run it from the repository root:

```powershell
python .\synthetic_pod\pod2_sim.py --gateway-host 127.0.0.1 --gateway-port 8765 --pod-id 02 --interval 10 --verbose
```

Useful fault-injection options:

- `--p-drop 0.1`
- `--p-corrupt 0.05`
- `--p-delay 0.2`
- `--p-disconnect 0.02`
- `--replay-buffer-size 300`

The simulator keeps a replay buffer so the gateway can request:

- `REQ_SEQ`
- `REQ_FROM_SEQ`

When a resend command arrives, the simulator replays the requested samples immediately in order.
