$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

function Resolve-Python {
  $py = Get-Command py -ErrorAction SilentlyContinue
  if ($py) { return @{ Kind = "py"; Path = $py.Source } }

  $python = Get-Command python -ErrorAction SilentlyContinue
  if (-not $python) { return $null }

  # Detect Microsoft Store shim (WindowsApps)
  if ($python.Source -like "*\\WindowsApps\\python.exe") {
    throw "Found Microsoft Store python shim at '$($python.Source)'. Install Python from python.org (with the 'py' launcher) or disable App execution aliases for python.exe/python3.exe."
  }

  return @{ Kind = "python"; Path = $python.Source }
}

$Python = Resolve-Python
if (-not $Python) {
  throw "Python not found. Install Python 3.11+ (recommended: python.org installer with 'py' launcher)."
}

if (-not (Test-Path ".\\.venv")) {
  Write-Host "Creating venv..."
  if ($Python.Kind -eq "py") {
    & py -3.11 -m venv .venv
  } else {
    & python -m venv .venv
  }
}

Write-Host "Installing requirements..."
& .\\.venv\\Scripts\\python.exe -m pip install -r requirements.txt | Out-Host

Write-Host "Starting API server..."
$proc = Start-Process -FilePath ".\\.venv\\Scripts\\python.exe" -ArgumentList @(
  "-m","uvicorn","backend_node_parity:app",
  "--host","127.0.0.1","--port","3255","--log-level","warning"
) -PassThru -WindowStyle Hidden

try {
  $healthUrl = "http://127.0.0.1:3255/api/health"
  $tableUrl = "http://127.0.0.1:3255/api/table-data"

  $deadline = (Get-Date).AddSeconds(15)
  do {
    Start-Sleep -Milliseconds 250
    try {
      $r = Invoke-RestMethod -Uri $healthUrl -Method GET -TimeoutSec 2
      if ($r.ok -eq $true) { break }
    } catch {
      # keep waiting
    }
  } while ((Get-Date) -lt $deadline)

  $r = Invoke-RestMethod -Uri $healthUrl -Method GET -TimeoutSec 5
  if ($r.ok -ne $true) { throw "Health check failed: $($r | ConvertTo-Json -Compress)" }
  Write-Host "Health: OK"

  $t = Invoke-RestMethod -Uri $tableUrl -Method GET -TimeoutSec 30
  $count = 0
  if ($t -and $t.rows) { $count = @($t.rows).Count }
  Write-Host "Table-data: OK (rows=$count)"

  Write-Host "Smoke test passed."
}
finally {
  if ($proc -and -not $proc.HasExited) {
    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
  }
}

