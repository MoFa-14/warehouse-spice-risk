param(
    [int]$GatewayPort = 8765,
    [string]$ZoneProfile = "entrance_disturbed"
)

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $repoRoot

$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
& $python ".\synthetic_pod\pod2_sim.py" --gateway-port $GatewayPort --interval 60 --zone-profile $ZoneProfile --verbose
