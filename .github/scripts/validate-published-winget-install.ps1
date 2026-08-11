param(
  [Parameter(Mandatory = $true)]
  [string]$ManifestRoot,

  [Parameter(Mandatory = $true)]
  [string]$ExpectedVersion
)

$ErrorActionPreference = "Stop"
$packageId = "ScryerMedia.Weaver"
$legacyPortableVersion = "0.7.4"

function Get-ProgramFiles64 {
  if ($env:ProgramW6432) {
    return $env:ProgramW6432
  }

  return ${env:ProgramFiles}
}

function Assert-PublishedMsiInstallation {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Context
  )

  $installDir = Join-Path (Get-ProgramFiles64) "Scryer Media\Weaver"
  $weaverExe = Join-Path $installDir "weaver.exe"
  $trayExe = Join-Path $installDir "weaver-tray.exe"
  foreach ($required in @($weaverExe, $trayExe)) {
    if (-not (Test-Path $required)) {
      throw "$Context installed the published MSI but did not install $required"
    }
  }
  if (Get-Process weaver-tray -ErrorAction SilentlyContinue) {
    throw "$Context started weaver-tray.exe."
  }

  $versionOutput = (& $weaverExe --version | Out-String).Trim()
  if ($LASTEXITCODE -ne 0) {
    throw "$Context weaver.exe --version failed with exit code $LASTEXITCODE."
  }
  if ($versionOutput -notmatch [regex]::Escape($ExpectedVersion)) {
    throw "$Context Weaver reported '$versionOutput', expected version $ExpectedVersion."
  }
}

function Install-PublishedMsiWithWinGet {
  param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("install", "upgrade")]
    [string]$Verb
  )

  for ($attempt = 1; $attempt -le 5; $attempt++) {
    if ($Verb -eq "install") {
      & $winget install --manifest $ManifestRoot --silent --accept-package-agreements --accept-source-agreements --disable-interactivity
    } else {
      & $winget upgrade --manifest $ManifestRoot --installer-type msi --uninstall-previous --silent --accept-package-agreements --accept-source-agreements --disable-interactivity
    }
    if ($LASTEXITCODE -eq 0) {
      return
    }

    if ($attempt -eq 5) {
      throw "winget $Verb of the published MSI failed with exit code $LASTEXITCODE."
    }
    Start-Sleep -Seconds 15
  }
}

function Get-ManifestMsiProductCode {
  $installerManifests = @(Get-ChildItem -LiteralPath $ManifestRoot -Recurse -File -Filter "$packageId.installer.yaml")
  if ($installerManifests.Count -ne 1) {
    throw "Expected exactly one $packageId installer manifest beneath $ManifestRoot, found $($installerManifests.Count)."
  }

  $productCodeLines = @(Select-String -LiteralPath $installerManifests[0].FullName -Pattern '^ProductCode:\s*(.+?)\s*$')
  if ($productCodeLines.Count -ne 1) {
    throw "Expected exactly one ProductCode in $($installerManifests[0].FullName), found $($productCodeLines.Count)."
  }

  return $productCodeLines[0].Matches[0].Groups[1].Value.Trim()
}

function Remove-WeaverMsi {
  param(
    [Parameter(Mandatory = $true)]
    [string]$ProductCode
  )

  $msiExec = Join-Path $env:SystemRoot "System32\msiexec.exe"
  $process = Start-Process -FilePath $msiExec -ArgumentList @("/x", $ProductCode, "/qn", "/norestart") -PassThru -Wait
  if ($process.ExitCode -notin @(0, 3010)) {
    throw "MSI cleanup for $ProductCode failed with exit code $($process.ExitCode)."
  }

  $installDir = Join-Path (Get-ProgramFiles64) "Scryer Media\Weaver"
  if (Test-Path $installDir) {
    throw "MSI cleanup for $ProductCode left $installDir behind."
  }
}

function Remove-WeaverPortablePackage {
  & $winget uninstall --id $packageId --exact --silent --accept-source-agreements --disable-interactivity
  if ($LASTEXITCODE -ne 0) {
    throw "winget portable cleanup failed with exit code $LASTEXITCODE."
  }
}

$winget = (Get-Command winget.exe -ErrorAction SilentlyContinue).Source
if (-not $winget) {
  throw "winget.exe was not found; published MSI install validation is required."
}
if (-not (Test-Path $ManifestRoot)) {
  throw "WinGet manifest root does not exist: $ManifestRoot"
}

& $winget settings --enable LocalManifestFiles
if ($LASTEXITCODE -ne 0) {
  throw "Unable to enable local manifest files in winget (exit code $LASTEXITCODE)."
}
& $winget validate --manifest $ManifestRoot --disable-interactivity
if ($LASTEXITCODE -ne 0) {
  throw "Published winget manifest validation failed with exit code $LASTEXITCODE."
}
$msiProductCode = Get-ManifestMsiProductCode

$directInstallCompleted = $false
try {
  Install-PublishedMsiWithWinGet -Verb install
  $directInstallCompleted = $true
  Assert-PublishedMsiInstallation -Context "winget install"
} finally {
  if ($directInstallCompleted) {
    # Local-manifest installations are not registered under the catalog source,
    # so `winget uninstall --id` cannot reliably find them. The manifest's MSI
    # ProductCode is the installation identity WinGet just used.
    Remove-WeaverMsi -ProductCode $msiProductCode
  }
}

$legacyProfile = Join-Path $env:LOCALAPPDATA "weaver"
$legacyMarker = Join-Path $legacyProfile "winget-transition-marker.txt"
$legacyLink = Join-Path $env:LOCALAPPDATA "Microsoft\WinGet\Links\weaver.exe"
$portablePackagesRoot = Join-Path $env:LOCALAPPDATA "Microsoft\WinGet\Packages"
$legacyInstallCompleted = $false
$transitionCompleted = $false
try {
  New-Item -ItemType Directory -Force -Path $legacyProfile | Out-Null
  "legacy portable data must remain untouched" | Set-Content $legacyMarker

  & $winget install --id $packageId --exact --version $legacyPortableVersion --source winget --accept-package-agreements --accept-source-agreements --disable-interactivity
  if ($LASTEXITCODE -ne 0) {
    throw "winget could not install legacy portable Weaver $legacyPortableVersion (exit code $LASTEXITCODE)."
  }
  $legacyInstallCompleted = $true
  if (-not (Test-Path $legacyLink)) {
    throw "legacy portable Weaver did not create $legacyLink"
  }

  Install-PublishedMsiWithWinGet -Verb upgrade
  $transitionCompleted = $true
  Assert-PublishedMsiInstallation -Context "winget portable-to-MSI upgrade"

  if (Test-Path $legacyLink) {
    throw "winget portable-to-MSI upgrade retained the portable command link $legacyLink"
  }
  $remainingPortablePackages = Get-ChildItem -LiteralPath $portablePackagesRoot -Directory -Filter "ScryerMedia.Weaver_*" -ErrorAction SilentlyContinue
  if ($remainingPortablePackages) {
    throw "winget portable-to-MSI upgrade retained portable package files: $($remainingPortablePackages.FullName -join ', ')"
  }
  if (-not (Test-Path $legacyMarker)) {
    throw "winget portable-to-MSI upgrade removed legacy user data at $legacyMarker"
  }
} finally {
  if ($transitionCompleted) {
    Remove-WeaverMsi -ProductCode $msiProductCode
  } elseif ($legacyInstallCompleted) {
    Remove-WeaverPortablePackage
  }
  Remove-Item -LiteralPath $legacyProfile -Recurse -Force -ErrorAction SilentlyContinue
}
