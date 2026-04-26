param(
    [string]$DriveLetter = "E"
)

# Compare the repo source-of-truth files to the CIRCUITPY copy so it is obvious
# whether the board is still running a stale deployment.
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

$allMatch = $true
foreach ($file in $files) {
    $src = Join-Path $sourceDir $file
    $dst = Join-Path $targetRoot $file
    if (-not (Test-Path $dst)) {
        Write-Host "MISSING  $file" -ForegroundColor Yellow
        $allMatch = $false
        continue
    }

    $srcHash = (Get-FileHash $src -Algorithm SHA256).Hash
    $dstHash = (Get-FileHash $dst -Algorithm SHA256).Hash
    if ($srcHash -eq $dstHash) {
        Write-Host "MATCH    $file" -ForegroundColor Green
    }
    else {
        Write-Host "DIFF     $file" -ForegroundColor Red
        $allMatch = $false
    }
}

if ($allMatch) {
    Write-Host "Board firmware matches repo source." -ForegroundColor Green
    exit 0
}

Write-Host "Board firmware does not match repo source." -ForegroundColor Yellow
exit 1
