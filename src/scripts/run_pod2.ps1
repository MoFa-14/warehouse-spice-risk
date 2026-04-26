param(
    [int]$GatewayPort = 8765,
    [string]$ZoneProfile = "entrance_disturbed",
    [int]$PodCount = 9,
    [int]$StartPodId = 2,
    [double]$DisconnectProbability = 0.02
)

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $repoRoot

& (Join-Path $PSScriptRoot "run_synthetic_pods.ps1") `
    -GatewayPort $GatewayPort `
    -ZoneProfile $ZoneProfile `
    -PodCount $PodCount `
    -StartPodId $StartPodId `
    -DisconnectProbability $DisconnectProbability
