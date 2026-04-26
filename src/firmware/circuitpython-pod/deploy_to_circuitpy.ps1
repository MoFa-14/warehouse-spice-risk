param(
    [string]$DriveLetter = "E"
)

# Copy the repo firmware files to the mounted CIRCUITPY drive. This script is
# intentionally small so the deployment step is obvious during demos.
$sourceDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$targetRoot = "$DriveLetter`:\"
$files = @(
    "code.py",
    "config.py",
    "ble_service.py",
    "sensors.py",
    "ring_buffer.py",
    "status.py"
)

if (-not (Test-Path $targetRoot)) {
    throw "Target drive $targetRoot not found."
}

Write-Host "Deploying pod firmware from $sourceDir to $targetRoot"
foreach ($file in $files) {
    $src = Join-Path $sourceDir $file
    $dst = Join-Path $targetRoot $file
    Write-Host "Copying $file"
    Copy-Item -Force $src $dst
}

Write-Host "Deployment complete."
Write-Host "Tip: press RESET once on the board or wait for auto-reload."
