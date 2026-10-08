$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "Starting RetailPulse pipeline..."
Write-Host ""

python .\orchestration\run_pipeline.py

if ($LASTEXITCODE -ne 0) {
    Write-Error "RetailPulse pipeline failed."
    exit $LASTEXITCODE
}

Write-Host ""
Write-Host "RetailPulse pipeline completed successfully."