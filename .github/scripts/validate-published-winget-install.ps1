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
  for ($attempt = 1; $attempt -le 5; $attempt++) {
    & $winget install --manifest $manifestDirectory --silent --accept-package-agreements --accept-source-agreements --disable-interactivity
    if ($LASTEXITCODE -eq 0) {
      return
    }

    if ($attempt -eq 5) {
      throw "winget install of the published MSI failed with exit code $LASTEXITCODE."
    }
    Start-Sleep -Seconds 15
  }
}

function Get-ManifestMsiProductCode {
  param(
    [Parameter(Mandatory = $true)]
    [string]$ManifestDirectory
  )

  $installerManifest = Join-Path $ManifestDirectory "$packageId.installer.yaml"
  if (-not (Test-Path $installerManifest)) {
    throw "WinGet installer manifest was not found at $installerManifest."
  }

  # The installer manifest lists one entry per architecture, each with its own
  # indented ProductCode; the smoke test runs on x64, so that entry is the
  # installation identity to parse out.
  $x64Installer = $false
  foreach ($line in Get-Content -LiteralPath $installerManifest) {
    if ($line -match '^\s*-\s*Architecture:\s*(?<architecture>\S+)\s*$') {
      $x64Installer = $Matches.architecture -eq "x64"
      continue
    }
    if ($x64Installer -and $line -match '^\s*ProductCode:\s*''?(?<productCode>\{[0-9A-Fa-f-]+\})''?\s*$') {
      return $Matches.productCode
    }
  }

  throw "WinGet installer manifest did not declare an x64 MSI ProductCode."
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

# winget's --manifest argument wants the directory that directly contains the
# manifest YAML files, and the packaged archive nests them under
# ScryerMedia.Weaver/<version>/, so resolve that leaf directory first.
$manifestDirectories = @(
  Get-ChildItem -Path $ManifestRoot -Recurse -File -Filter "*.yaml" |
    ForEach-Object { $_.DirectoryName } |
    Sort-Object -Unique
)
if ($manifestDirectories.Count -ne 1) {
  throw "Expected exactly one directory containing WinGet manifest YAML files below $ManifestRoot; found $($manifestDirectories.Count)."
}
$manifestDirectory = $manifestDirectories[0]

& $winget settings --enable LocalManifestFiles
if ($LASTEXITCODE -ne 0) {
  throw "Unable to enable local manifest files in winget (exit code $LASTEXITCODE)."
}
& $winget validate --manifest $manifestDirectory --disable-interactivity
$manifestValidationExitCode = $LASTEXITCODE
if ($manifestValidationExitCode -eq -1978335192) {
  Write-Warning "Published winget manifest validation succeeded with warnings. Continuing to the install smoke test."
} elseif ($manifestValidationExitCode -ne 0) {
  throw "Published winget manifest validation failed with exit code $manifestValidationExitCode."
}
$msiProductCode = Get-ManifestMsiProductCode -ManifestDirectory $manifestDirectory

$directInstallCompleted = $false
try {
  Install-PublishedMsiWithWinGet
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
$portableRemoved = $false
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

  # winget refuses to upgrade across install technologies ("The install
  # technology of the newer version specified is different from the current
  # version installed"), so a portable-to-MSI `winget upgrade` can never
  # succeed. Validate the transition winget actually prescribes for legacy
  # portable users: uninstall the portable package, then install the MSI.
  Remove-WeaverPortablePackage
  $portableRemoved = $true
  Install-PublishedMsiWithWinGet
  $transitionCompleted = $true
  Assert-PublishedMsiInstallation -Context "winget portable-to-MSI transition"

  if (Test-Path $legacyLink) {
    throw "winget portable-to-MSI transition retained the portable command link $legacyLink"
  }
  $remainingPortablePackages = Get-ChildItem -LiteralPath $portablePackagesRoot -Directory -Filter "ScryerMedia.Weaver_*" -ErrorAction SilentlyContinue
  if ($remainingPortablePackages) {
    throw "winget portable-to-MSI transition retained portable package files: $($remainingPortablePackages.FullName -join ', ')"
  }
  if (-not (Test-Path $legacyMarker)) {
    throw "winget portable-to-MSI transition removed legacy user data at $legacyMarker"
  }
} finally {
  if ($transitionCompleted) {
    Remove-WeaverMsi -ProductCode $msiProductCode
  } elseif ($legacyInstallCompleted -and -not $portableRemoved) {
    Remove-WeaverPortablePackage
  }
  Remove-Item -LiteralPath $legacyProfile -Recurse -Force -ErrorAction SilentlyContinue
}
