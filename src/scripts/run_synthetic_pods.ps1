param(
    [int]$GatewayPort = 8765,
    [string]$ZoneProfile = "entrance_disturbed",
    [int]$PodCount = 9,
    [int]$StartPodId = 2,
    [double]$DisconnectProbability = 0.02
)

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $repoRoot

$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
$podIdStartText = "{0:D2}" -f $StartPodId

& $python ".\synthetic_pod\pod2_sim.py" `
    --gateway-port $GatewayPort `
    --interval 60 `
    --zone-profile $ZoneProfile `
    --pod-count $PodCount `
    --pod-id-start $podIdStartText `
    --p-disconnect $DisconnectProbability `
    --verbose
