param(
    [switch]$Commit,
    [switch]$Push,
    [string]$PythonExe = "python",
    [string]$CommitMessage = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$LandingRepo = Join-Path $RepoRoot "landing-ventas"
$LandingData = Join-Path $LandingRepo "data\\landing-data.json"
$ExportScript = Join-Path $RepoRoot "export_landing_data.py"

if (-not (Test-Path -LiteralPath $ExportScript)) {
    throw "No se encontró export_landing_data.py en $RepoRoot"
}

if (-not (Test-Path -LiteralPath (Join-Path $LandingRepo ".git"))) {
    throw "No se encontró el repo git de la landing en $LandingRepo"
}

Write-Host "Generando JSON para la landing..." -ForegroundColor Cyan
& $PythonExe $ExportScript
if ($LASTEXITCODE -ne 0) {
    throw "Falló la generación del JSON de la landing."
}

if (-not (Test-Path -LiteralPath $LandingData)) {
    throw "No se generó el archivo esperado: $LandingData"
}

Write-Host "JSON generado en $LandingData" -ForegroundColor Green

if (-not $Commit -and -not $Push) {
    Write-Host "Sin commit ni push. Proceso terminado." -ForegroundColor Yellow
    exit 0
}

Write-Host "Preparando commit en el repo de la landing..." -ForegroundColor Cyan
& git -C $LandingRepo add -- "data/landing-data.json"
if ($LASTEXITCODE -ne 0) {
    throw "No se pudo hacer git add del JSON de la landing."
}

& git -C $LandingRepo diff --cached --quiet -- "data/landing-data.json"
$HasStagedChanges = ($LASTEXITCODE -ne 0)

if (-not $HasStagedChanges) {
    Write-Host "No hay cambios nuevos en data/landing-data.json." -ForegroundColor Yellow
    exit 0
}

if ([string]::IsNullOrWhiteSpace($CommitMessage)) {
    $CommitMessage = "Update landing data $(Get-Date -Format 'yyyy-MM-dd HH:mm')"
}

& git -C $LandingRepo commit -m $CommitMessage -- "data/landing-data.json"
if ($LASTEXITCODE -ne 0) {
    throw "No se pudo crear el commit del JSON de la landing."
}

Write-Host "Commit creado en el repo de la landing." -ForegroundColor Green

if ($Push) {
    Write-Host "Enviando cambios a origin..." -ForegroundColor Cyan
    & git -C $LandingRepo push
    if ($LASTEXITCODE -ne 0) {
        throw "No se pudo hacer push del repo de la landing."
    }
    Write-Host "Push completado." -ForegroundColor Green
}
