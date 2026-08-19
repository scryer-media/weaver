[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [ValidateSet("x64", "arm64")]
  [string]$Architecture,

  [Parameter(Mandatory = $true)]
  [string]$StageDir,

  [Parameter(Mandatory = $true)]
  [string]$Version,

  [Parameter(Mandatory = $true)]
  [string]$OutputPath
)

$ErrorActionPreference = "Stop"

$upgradeCodes = @{
  x64 = "{694CA1CE-CB74-486A-BB1A-005D1D2051A2}"
  arm64 = "{AD8E9924-5148-4052-9A91-E4B7B47C9CD7}"
}

# Component GUIDs are stable within an architecture so Windows Installer can
# service upgrades, while x64 and ARM64 retain independent component identity.
$componentGuids = @{
  x64 = @{
    applicationFiles = "{248CAD44-E568-4FCF-8372-A238EA9ED001}"
    startMenuShortcuts = "{EA708F49-D1FA-46BB-8035-EDB0F0CD2ECD}"
  }
  arm64 = @{
    applicationFiles = "{A52D4F1B-9043-4C4B-ADD6-8A221CD1A158}"
    startMenuShortcuts = "{7A0A64E8-F1C1-41D3-A201-04772ED043BC}"
  }
}

if ($Version -notmatch '^\d+\.\d+\.\d+$') {
  throw "Windows MSI version must be release-derived major.minor.patch, got '$Version'."
}

$stageDir = (Resolve-Path -LiteralPath $StageDir).Path
foreach ($required in @("weaver.exe", "weaver-tray.exe", "weaver.ico", "LICENSE")) {
  if (-not (Test-Path (Join-Path $stageDir $required))) {
    throw "MSI staging directory is missing ${required}: $stageDir"
  }
}

$wix = Get-Command wix.exe -ErrorAction SilentlyContinue
if (-not $wix) {
  $wix = Get-Command wix -ErrorAction SilentlyContinue
}
if (-not $wix) {
  throw "WiX v4 CLI was not found on PATH. Install the pinned wix dotnet tool before packaging."
}

$source = Join-Path $PSScriptRoot "weaver.wxs"
$outputPath = [System.IO.Path]::GetFullPath($OutputPath)
$outputDir = Split-Path -Parent $outputPath
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

& $wix.Source build `
  -arch $Architecture `
  -d "StageDir=$stageDir" `
  -d "ProductVersion=$Version" `
  -d "UpgradeCode=$($upgradeCodes[$Architecture])" `
  -d "ApplicationFilesComponentGuid=$($componentGuids[$Architecture].applicationFiles)" `
  -d "StartMenuShortcutsComponentGuid=$($componentGuids[$Architecture].startMenuShortcuts)" `
  -o $outputPath `
  $source
if ($LASTEXITCODE -ne 0) {
  throw "WiX failed to build $outputPath with exit code $LASTEXITCODE."
}

$installer = New-Object -ComObject WindowsInstaller.Installer
$database = $installer.OpenDatabase($outputPath, 0)
$view = $database.OpenView("SELECT `Value` FROM `Property` WHERE `Property`='ProductCode'")
$view.Execute()
$record = $view.Fetch()
if (-not $record) {
  throw "WiX MSI did not contain a ProductCode property: $outputPath"
}
$productCode = $record.StringData(1)
$view.Close()

[ordered]@{
  architecture = $Architecture
  product_code = $productCode
  upgrade_code = $upgradeCodes[$Architecture]
  version = $Version
} | ConvertTo-Json | Set-Content -Encoding utf8 "$outputPath.json"

Write-Host "Built $outputPath with ProductCode $productCode"
