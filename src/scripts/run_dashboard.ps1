param(
    [int]$Port = 5000,
    [string]$HostAddress = "127.0.0.1",
    [int]$AutoRefreshSeconds = 0
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$dashboardRoot = Join-Path $repoRoot "dashboard"
$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $venvPython)) {
    throw "Python virtual environment not found at $venvPython"
}

Push-Location $dashboardRoot
try {
    $env:DASHBOARD_AUTO_REFRESH_SECONDS = "$AutoRefreshSeconds"
    & $venvPython -m flask --app app.main run --host $HostAddress --port $Port --debug
}
finally {
    Pop-Location
}
