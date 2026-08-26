param(
  [Parameter(Mandatory = $true)]
  [string]$Architecture,

  [Parameter(Mandatory = $true)]
  [string]$ZipPath,

  [Parameter(Mandatory = $true)]
  [string]$MsiPath,

  [Parameter(Mandatory = $true)]
  [string]$MsiMetadataPath,

  [Parameter(Mandatory = $true)]
  [string]$BuiltExePath,

  [Parameter(Mandatory = $true)]
  [string]$BuiltTrayPath
)

$ErrorActionPreference = "Stop"

$prefix = "weaver-windows-$Architecture"
$defenderLog = "$prefix-defender-scan.log"
$attachmentLog = "$prefix-attachment-services.log"
$startupLog = "$prefix-noarg-startup.log"
$trayStdoutLog = "$prefix-tray-stdout.log"
$trayStderrLog = "$prefix-tray-stderr.log"
$wingetLog = "$prefix-winget-install.log"
$validationTempRoot = if ($env:RUNNER_TEMP) { $env:RUNNER_TEMP } else { [System.IO.Path]::GetTempPath() }
$validationRoot = Join-Path $validationTempRoot "weaver-package-validation-$Architecture"
$validationEncryptionKey = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="

function Write-Log {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Path,

    [Parameter(Mandatory = $true)]
    [string]$Message
  )

  $line = "$(Get-Date -Format o) $Message"
  Write-Host $line
  Add-Content -Path $Path -Value $line
}

function Reset-NativeExitCode {
  $global:LASTEXITCODE = 0
}

function Restore-EnvVar {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Name,

    [AllowNull()]
    [string]$Value
  )

  if ($null -eq $Value) {
    Remove-Item -Path "Env:$Name" -ErrorAction SilentlyContinue
  } else {
    Set-Item -Path "Env:$Name" -Value $Value
  }
}

function Get-ProgramFiles64 {
  if ($env:ProgramW6432) {
    return $env:ProgramW6432
  }

  return ${env:ProgramFiles}
}

function Invoke-MsiExec {
  param(
    [Parameter(Mandatory = $true)]
    [string[]]$Arguments
  )

  # The package is x64-only. Sysnative ensures a mistakenly launched 32-bit
  # PowerShell host still invokes the native 64-bit Windows Installer.
  $msiExecDirectory = if ([Environment]::Is64BitProcess) { "System32" } else { "Sysnative" }
  $msiExec = Join-Path $env:WINDIR "$msiExecDirectory\msiexec.exe"
  $process = Start-Process -FilePath $msiExec -ArgumentList $Arguments -PassThru -Wait
  return $process.ExitCode
}

function Test-InteractiveDesktop {
  $currentSession = (Get-Process -Id $PID).SessionId
  $explorer = Get-Process explorer -ErrorAction SilentlyContinue |
    Where-Object { $_.SessionId -eq $currentSession } |
    Select-Object -First 1
  if ($null -eq $explorer) {
    return $false
  }

  # Some hosted Windows ARM runners start explorer.exe without a taskbar. That
  # is not a usable GUI session: the tray icon cannot be created and the tray
  # process never reaches server startup. Require the actual notification-area
  # owner before running the GUI-only smoke.
  if (-not ("WeaverNativeWindow" -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;

public static class WeaverNativeWindow {
  [DllImport("user32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
  public static extern IntPtr FindWindow(string className, string windowName);

  [DllImport("user32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
  public static extern IntPtr FindWindowEx(
    IntPtr parent,
    IntPtr childAfter,
    string className,
    string windowName
  );
}
'@
  }

  $taskbar = [WeaverNativeWindow]::FindWindow("Shell_TrayWnd", $null)
  if ($taskbar -eq [IntPtr]::Zero) {
    return $false
  }

  return [WeaverNativeWindow]::FindWindowEx(
    $taskbar,
    [IntPtr]::Zero,
    "TrayNotifyWnd",
    $null
  ) -ne [IntPtr]::Zero
}

function Get-MpCmdRun {
  $candidates = @()
  $programFiles = ${env:ProgramFiles}
  if ($programFiles) {
    $candidates += Join-Path $programFiles "Windows Defender\MpCmdRun.exe"
  }
  $platformRoot = Join-Path $env:ProgramData "Microsoft\Windows Defender\Platform"
  if (Test-Path $platformRoot) {
    $candidates += Get-ChildItem $platformRoot -Recurse -Filter MpCmdRun.exe -ErrorAction SilentlyContinue |
      Sort-Object FullName -Descending |
      Select-Object -ExpandProperty FullName
  }

  foreach ($candidate in $candidates) {
    if ($candidate -and (Test-Path $candidate)) {
      return $candidate
    }
  }

  return $null
}

function Invoke-DefenderScan {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Path
  )

  $mp = Get-MpCmdRun
  if (-not $mp) {
    Write-Log $defenderLog "MpCmdRun.exe was not found; Defender scan skipped."
    return
  }

  Write-Log $defenderLog "Scanning $Path with $mp"
  & $mp -Scan -ScanType 3 -File $Path -DisableRemediation *>> $defenderLog
  if ($LASTEXITCODE -ne 0) {
    throw "Defender scan failed for $Path with exit code $LASTEXITCODE. See $defenderLog."
  }
}

function Invoke-AttachmentServicesSave {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Path,

    [Parameter(Mandatory = $true)]
    [string]$Source
  )

  Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;

[ComImport]
[Guid("4125dd96-e03a-4103-8f70-e0597d803b9c")]
public class AttachmentServices
{
}

[ComImport]
[Guid("73db1241-1e85-4581-8e4f-a81e1d0f8c57")]
[InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
public interface IAttachmentExecute
{
    void SetClientTitle([MarshalAs(UnmanagedType.LPWStr)] string clientTitle);
    void SetClientGuid(ref Guid guid);
    void SetLocalPath([MarshalAs(UnmanagedType.LPWStr)] string localPath);
    void SetFileName([MarshalAs(UnmanagedType.LPWStr)] string fileName);
    void SetSource([MarshalAs(UnmanagedType.LPWStr)] string source);
    void SetReferrer([MarshalAs(UnmanagedType.LPWStr)] string referrer);
    [PreserveSig] int CheckPolicy();
    [PreserveSig] int Prompt(IntPtr parent, int prompt, out int action);
    [PreserveSig] int Save();
    [PreserveSig] int Execute(IntPtr parent, [MarshalAs(UnmanagedType.LPWStr)] string verb, IntPtr processHandle);
    [PreserveSig] int SaveWithUI(IntPtr parent);
    void ClearClientState();
}

public static class WeaverAttachmentValidation
{
    public static int Save(string localPath, string source)
    {
        IAttachmentExecute attachment = (IAttachmentExecute)new AttachmentServices();
        attachment.SetLocalPath(localPath);
        attachment.SetFileName(System.IO.Path.GetFileName(localPath));
        attachment.SetSource(source);
        return attachment.Save();
    }
}
"@

  Write-Log $attachmentLog "Calling IAttachmentExecute::Save for $Path from $Source"
  $hr = [WeaverAttachmentValidation]::Save($Path, $Source)
  $unsigned = [uint32]$hr
  Write-Log $attachmentLog ("IAttachmentExecute::Save HRESULT: 0x{0:X8}" -f $unsigned)
  if ($hr -ne 0) {
    throw "Attachment Services rejected $Path with HRESULT 0x$($unsigned.ToString("X8")). See $attachmentLog."
  }
}

function Invoke-NoArgStartupSmoke {
  param(
    [Parameter(Mandatory = $true)]
    [string]$ExePath
  )

  $startupRoot = Join-Path $validationRoot "noarg-startup"
  $workDir = Join-Path $startupRoot "cwd"
  $localAppData = Join-Path $startupRoot "local-app-data"
  $appData = Join-Path $startupRoot "roaming-app-data"
  New-Item -ItemType Directory -Force -Path $workDir, $localAppData, $appData | Out-Null

  $oldLocalAppData = $env:LOCALAPPDATA
  $oldAppData = $env:APPDATA
  $oldBindAddress = $env:WEAVER_HTTP_BIND_ADDRESS
  $oldEncryptionKey = $env:WEAVER_ENCRYPTION_KEY
  $process = $null

  try {
    $env:LOCALAPPDATA = $localAppData
    $env:APPDATA = $appData
    $env:WEAVER_HTTP_BIND_ADDRESS = "127.0.0.1"
    # GitHub Actions and OpenSSH processes do not have an interactive Windows
    # logon session, so Credential Manager is intentionally unavailable there.
    # Use a fixed test-only key to exercise first-run and restart behavior.
    $env:WEAVER_ENCRYPTION_KEY = $validationEncryptionKey

    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Parse("127.0.0.1"), 9090)
    try {
      $listener.Start()
    } finally {
      $listener.Stop()
    }

    Write-Log $startupLog "Starting $ExePath with no arguments from $workDir"
    $process = Start-Process -FilePath $ExePath -WorkingDirectory $workDir -PassThru
    $session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
    $deadline = (Get-Date).AddSeconds(30)
    $lastError = $null
    $ready = $false

    while ((Get-Date) -lt $deadline) {
      if ($process.HasExited) {
        throw "Weaver exited before no-arg API readiness with code $($process.ExitCode)"
      }

      try {
        $spa = Invoke-WebRequest -Uri "http://127.0.0.1:9090/" -WebSession $session -TimeoutSec 5 -UseBasicParsing
        if ($spa.StatusCode -ne 200) {
          throw "SPA returned HTTP $($spa.StatusCode)"
        }

        $readiness = Invoke-WebRequest -Uri "http://127.0.0.1:9090/readyz" -WebSession $session -TimeoutSec 5 -UseBasicParsing
        if ($readiness.StatusCode -ne 200) {
          throw "readiness probe returned HTTP $($readiness.StatusCode)"
        }

        $payload = @{ query = "query { systemStatus { version } }" } | ConvertTo-Json -Compress
        $api = Invoke-WebRequest -Uri "http://127.0.0.1:9090/graphql" -Method Post -ContentType "application/json" -Body $payload -WebSession $session -TimeoutSec 5 -UseBasicParsing -SkipHttpErrorCheck
        if ([int]$api.StatusCode -ne 401) {
          throw "unauthenticated GraphQL returned HTTP $($api.StatusCode), expected 401"
        }

        Write-Log $startupLog "No-arg startup API smoke passed; readiness is healthy and GraphQL authentication is enforced"
        $ready = $true
        break
      } catch {
        $lastError = $_.Exception.Message
        Start-Sleep -Milliseconds 500
      }
    }

    if (-not $ready) {
      throw "Timed out waiting for no-arg Weaver API readiness. Last error: $lastError"
    }

    $cwdDb = Join-Path $workDir "weaver.db"
    if (Test-Path $cwdDb) {
      throw "No-arg startup created $cwdDb; expected app-data config root."
    }

    $appDataDb = Join-Path $localAppData "weaver\weaver.db"
    if (-not (Test-Path $appDataDb)) {
      throw "No-arg startup did not create expected app-data database at $appDataDb."
    }
  } finally {
    if ($process -and -not $process.HasExited) {
      Stop-Process -Id $process.Id -ErrorAction SilentlyContinue
      try {
        Wait-Process -Id $process.Id -Timeout 10 -ErrorAction Stop
      } catch {
        Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
      }
    }

    $defaultLog = Join-Path $localAppData "weaver\logs\weaver.log"
    if (Test-Path $defaultLog) {
      Add-Content -Path $startupLog -Value "----- default Weaver log tail -----"
      Get-Content $defaultLog -Tail 160 | Add-Content -Path $startupLog
      Add-Content -Path $startupLog -Value "----- end default Weaver log tail -----"
    } else {
      Write-Log $startupLog "Default Weaver log was not created at $defaultLog"
    }

    Restore-EnvVar -Name "LOCALAPPDATA" -Value $oldLocalAppData
    Restore-EnvVar -Name "APPDATA" -Value $oldAppData
    Restore-EnvVar -Name "WEAVER_HTTP_BIND_ADDRESS" -Value $oldBindAddress
    Restore-EnvVar -Name "WEAVER_ENCRYPTION_KEY" -Value $oldEncryptionKey
  }
}

function Assert-NoArgKeyPersistence {
  $defaultLog = Join-Path $validationRoot "noarg-startup\local-app-data\weaver\logs\weaver.log"
  if (-not (Test-Path $defaultLog)) {
    throw "Cannot verify no-arg encryption-key persistence because $defaultLog does not exist."
  }

  $contents = Get-Content $defaultLog -Raw
  if ($contents -notmatch "using encryption master key from WEAVER_ENCRYPTION_KEY") {
    throw "A repeated no-arg startup did not use the explicit validation encryption key."
  }
  Write-Log $startupLog "Repeated no-arg startup used the explicit validation encryption key."
}

function Write-WinGetMsiManifest {
  param(
    [Parameter(Mandatory = $true)]
    [string]$ManifestRoot,

    [Parameter(Mandatory = $true)]
    [string]$PackageMsi,

    [Parameter(Mandatory = $true)]
    [string]$InstallerUrl,

    [Parameter(Mandatory = $true)]
    [string]$ProductCode,

    [Parameter(Mandatory = $true)]
    [string]$PackageVersion
  )

  New-Item -ItemType Directory -Force -Path $manifestRoot | Out-Null
  $msiHash = (Get-FileHash $PackageMsi -Algorithm SHA256).Hash.ToUpperInvariant()
  $wingetArchitecture = if ($Architecture -eq "x86_64") { "x64" } else { "arm64" }

  @"
# yaml-language-server: `$schema=https://aka.ms/winget-manifest.version.1.10.0.schema.json
PackageIdentifier: ScryerMedia.Weaver
PackageVersion: $PackageVersion
DefaultLocale: en-US
ManifestType: version
ManifestVersion: 1.10.0
"@ | Set-Content -Path (Join-Path $manifestRoot "ScryerMedia.Weaver.yaml") -Encoding utf8

  @"
# yaml-language-server: `$schema=https://aka.ms/winget-manifest.defaultLocale.1.10.0.schema.json
PackageIdentifier: ScryerMedia.Weaver
PackageVersion: $PackageVersion
PackageLocale: en-US
Publisher: Scryer Media
PackageName: Weaver
License: GPL-3.0
ShortDescription: High-performance Usenet binary downloader.
ManifestType: defaultLocale
ManifestVersion: 1.10.0
"@ | Set-Content -Path (Join-Path $manifestRoot "ScryerMedia.Weaver.locale.en-US.yaml") -Encoding utf8

  @"
# yaml-language-server: `$schema=https://aka.ms/winget-manifest.installer.1.10.0.schema.json
PackageIdentifier: ScryerMedia.Weaver
PackageVersion: $PackageVersion
InstallerType: msi
UpgradeBehavior: uninstallPrevious
Installers:
- Architecture: $wingetArchitecture
  InstallerUrl: $InstallerUrl
  InstallerSha256: $msiHash
  ProductCode: '$ProductCode'
ManifestType: installer
ManifestVersion: 1.10.0
"@ | Set-Content -Path (Join-Path $manifestRoot "ScryerMedia.Weaver.installer.yaml") -Encoding utf8
}

function Invoke-WinGetManifestValidation {
  param(
    [Parameter(Mandatory = $true)]
    [string]$PackageMsi,

    [Parameter(Mandatory = $true)]
    [string]$ProductCode,

    [Parameter(Mandatory = $true)]
    [string]$PackageVersion
  )

  $winget = (Get-Command winget.exe -ErrorAction SilentlyContinue).Source
  if (-not $winget) {
    throw "winget.exe was not found; MSI manifest validation is required."
  }

  $manifestRoot = Join-Path $validationRoot "winget-manifest"
  $installerUrl = "https://github.com/scryer-media/weaver/releases/download/weaver-local-ci/weaver-windows-$Architecture.msi"
  Write-WinGetMsiManifest `
    -ManifestRoot $manifestRoot `
    -PackageMsi $PackageMsi `
    -InstallerUrl $installerUrl `
    -ProductCode $ProductCode `
    -PackageVersion $PackageVersion

  Write-Log $wingetLog "Validating the generated MSI winget manifest from $manifestRoot"
  & $winget validate --manifest $manifestRoot --disable-interactivity *>> $wingetLog
  if ($LASTEXITCODE -ne 0) {
    throw "winget manifest validation exited with code $LASTEXITCODE"
  }
  Write-Log $wingetLog "Generated MSI winget manifest validation succeeded."
}
Remove-Item -Recurse -Force $validationRoot -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $validationRoot | Out-Null
"" | Set-Content $defenderLog
"" | Set-Content $attachmentLog
"" | Set-Content $startupLog
"" | Set-Content $trayStdoutLog
"" | Set-Content $trayStderrLog
"" | Set-Content $wingetLog

$zipCopy = Join-Path $validationRoot (Split-Path $ZipPath -Leaf)
$msiCopy = Join-Path $validationRoot (Split-Path $MsiPath -Leaf)
$extractRoot = Join-Path $validationRoot "extracted"
Copy-Item $ZipPath $zipCopy -Force
Copy-Item $MsiPath $msiCopy -Force
Expand-Archive -Path $zipCopy -DestinationPath $extractRoot -Force
$packagedExe = Join-Path $extractRoot "weaver.exe"
$packagedTray = Join-Path $extractRoot "weaver-tray.exe"
if (-not (Test-Path $packagedExe)) {
  throw "Packaged zip did not contain weaver.exe at the zip root."
}
if (-not (Test-Path $packagedTray)) {
  throw "Packaged zip did not contain weaver-tray.exe at the zip root."
}

$builtHash = (Get-FileHash $BuiltExePath -Algorithm SHA256).Hash
$packagedHash = (Get-FileHash $packagedExe -Algorithm SHA256).Hash
if ($builtHash -ne $packagedHash) {
  throw "Packaged weaver.exe hash differs from built executable."
}
$builtTrayHash = (Get-FileHash $BuiltTrayPath -Algorithm SHA256).Hash
$packagedTrayHash = (Get-FileHash $packagedTray -Algorithm SHA256).Hash
if ($builtTrayHash -ne $packagedTrayHash) {
  throw "Packaged weaver-tray.exe hash differs from built executable."
}

foreach ($unsignedArtifact in @($packagedExe, $packagedTray, $msiCopy)) {
  $signature = Get-AuthenticodeSignature -FilePath $unsignedArtifact
  if ($signature.Status -ne "NotSigned") {
    throw "Expected intentionally unsigned artifact $unsignedArtifact, got Authenticode status $($signature.Status)."
  }
}

$msiMetadata = Get-Content $MsiMetadataPath -Raw | ConvertFrom-Json
if ($msiMetadata.product_code -notmatch '^\{[0-9A-Fa-f]{8}(-[0-9A-Fa-f]{4}){3}-[0-9A-Fa-f]{12}\}$') {
  throw "MSI metadata did not contain a valid ProductCode: $($msiMetadata.product_code)"
}
if ($msiMetadata.version -notmatch '^\d+\.\d+\.\d+$') {
  throw "MSI metadata did not contain a valid release version: $($msiMetadata.version)"
}

Invoke-DefenderScan -Path $zipCopy
Invoke-DefenderScan -Path $packagedExe
Invoke-DefenderScan -Path $packagedTray
Invoke-DefenderScan -Path $msiCopy

$sourceUrl = "https://github.com/scryer-media/weaver/releases/download/weaver-local-ci/$(Split-Path $zipCopy -Leaf)"
Invoke-AttachmentServicesSave -Path $zipCopy -Source $sourceUrl
Invoke-AttachmentServicesSave -Path $msiCopy -Source ($sourceUrl -replace 'zip$', 'msi')

Invoke-NoArgStartupSmoke -ExePath $packagedExe
Invoke-NoArgStartupSmoke -ExePath $packagedExe
Assert-NoArgKeyPersistence

$desktopProfile = Join-Path $env:LOCALAPPDATA "ScryerMedia\Weaver"
$profileMarker = Join-Path $desktopProfile "preserve-on-uninstall.txt"
New-Item -ItemType Directory -Force -Path $desktopProfile | Out-Null
"preserve me" | Set-Content $profileMarker
$msiLog = "$prefix-msi-install.log"
$msiExitCode = Invoke-MsiExec -Arguments @("/i", $msiCopy, "/qn", "/norestart", "/l*v", $msiLog)
if ($msiExitCode -ne 0) {
  throw "MSI install failed with exit code $msiExitCode. See $msiLog."
}

$installDir = Join-Path (Get-ProgramFiles64) "Scryer Media\Weaver"
$installedExe = Join-Path $installDir "weaver.exe"
$installedTray = Join-Path $installDir "weaver-tray.exe"
foreach ($required in @($installedExe, $installedTray, (Join-Path $installDir "LICENSE"))) {
  if (-not (Test-Path $required)) {
    throw "MSI did not install expected payload file $required"
  }
}
if ((Get-FileHash $installedExe -Algorithm SHA256).Hash -ne $builtHash) {
  throw "MSI-installed weaver.exe hash differs from the built executable."
}
if ((Get-FileHash $installedTray -Algorithm SHA256).Hash -ne $builtTrayHash) {
  throw "MSI-installed weaver-tray.exe hash differs from the built executable."
}
if (-not (Test-Path (Join-Path $env:ProgramData "Microsoft\Windows\Start Menu\Programs\Weaver\Weaver.lnk"))) {
  throw "MSI did not create the Weaver Start Menu shortcut."
}
if (Get-Process weaver-tray -ErrorAction SilentlyContinue) {
  throw "Silent MSI install started weaver-tray.exe; silent installs must stay quiet."
}
if (([Environment]::GetEnvironmentVariable("Path", "Machine")) -match [regex]::Escape($installDir)) {
  throw "MSI added its install directory to the machine PATH."
}
if (Get-CimInstance Win32_Service | Where-Object { $_.PathName -match [regex]::Escape($installDir) }) {
  throw "MSI registered a Windows service for Weaver."
}

& $installedExe --version *>> $msiLog
if ($LASTEXITCODE -ne 0) {
  throw "MSI-installed weaver.exe --version failed with exit code $LASTEXITCODE."
}

if (Test-InteractiveDesktop) {
  $tray = $null
  $oldTrayEncryptionKey = $env:WEAVER_ENCRYPTION_KEY
  try {
  # GitHub-hosted Windows runners do not provide Credential Manager to this
  # session. Match the no-argument smoke and keep the tray server in the
  # deterministic CI-only key path.
  $env:WEAVER_ENCRYPTION_KEY = $validationEncryptionKey
  $tray = Start-Process -FilePath $installedTray -ArgumentList "--login-start" -PassThru `
    -RedirectStandardOutput $trayStdoutLog -RedirectStandardError $trayStderrLog
  $deadline = (Get-Date).AddSeconds(30)
  $trayReady = $false
  while ((Get-Date) -lt $deadline) {
    if ($tray.HasExited) {
      throw "Tray exited before Weaver became ready (exit code $($tray.ExitCode)). See $trayStdoutLog and $trayStderrLog."
    }
    try {
      $response = Invoke-WebRequest -Uri "http://127.0.0.1:9090/" -TimeoutSec 2 -UseBasicParsing
      if ($response.StatusCode -eq 200) {
        $trayReady = $true
        break
      }
    } catch {
      Start-Sleep -Milliseconds 250
    }
  }
  if (-not $trayReady) {
    throw "Tray did not make Weaver ready within 30 seconds. See $trayStdoutLog and $trayStderrLog."
  }
  if (-not (Test-Path (Join-Path $desktopProfile "weaver.db"))) {
    throw "Tray launch did not create the isolated desktop profile at $desktopProfile."
  }
  & $installedTray --shutdown *>> $msiLog
  if ($LASTEXITCODE -ne 0) {
    throw "Tray shutdown failed with exit code $LASTEXITCODE."
  }
  try { Wait-Process -Id $tray.Id -Timeout 15 -ErrorAction Stop } catch { throw "Tray did not exit after shutdown." }
  } finally {
    if ($tray -and -not $tray.HasExited) {
      & $installedTray --shutdown *>> $msiLog
      try { Wait-Process -Id $tray.Id -Timeout 15 -ErrorAction Stop } catch { Stop-Process -Id $tray.Id -Force -ErrorAction SilentlyContinue }
    }
    Restore-EnvVar -Name "WEAVER_ENCRYPTION_KEY" -Value $oldTrayEncryptionKey
  }
} else {
  Write-Log $msiLog "Skipping GUI tray startup smoke: this process session has no usable notification area."
}

$msiExitCode = Invoke-MsiExec -Arguments @("/fa", $msiCopy, "/qn", "/norestart", "/l*v", $msiLog)
if ($msiExitCode -ne 0) {
  throw "MSI repair failed with exit code $msiExitCode. See $msiLog."
}
$msiExitCode = Invoke-MsiExec -Arguments @("/x", $msiMetadata.product_code, "/qn", "/norestart", "/l*v", $msiLog)
if ($msiExitCode -ne 0) {
  throw "MSI uninstall failed with exit code $msiExitCode. See $msiLog."
}
if (Test-Path $installDir) {
  throw "MSI uninstall retained the Program Files payload directory $installDir."
}
if (-not (Test-Path $profileMarker)) {
  throw "MSI uninstall removed desktop user data at $profileMarker."
}

Invoke-WinGetManifestValidation `
  -PackageMsi $msiCopy `
  -ProductCode $msiMetadata.product_code `
  -PackageVersion $msiMetadata.version
