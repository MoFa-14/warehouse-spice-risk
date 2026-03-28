param(
    [string]$BleAddress = "",
    [int]$TcpPort = 8765,
    [int]$Duration = 0
)

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $repoRoot

$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
$args = @(
    "-m", "gateway.cli.gateway_cli", "multi",
    "--tcp-port", $TcpPort,
    "--interval-s", "10",
    "--verbose"
)

if ($BleAddress) {
    $args += @("--ble-address", $BleAddress)
}

if ($Duration -gt 0) {
    $args += @("--duration", $Duration)
}

& $python @args
