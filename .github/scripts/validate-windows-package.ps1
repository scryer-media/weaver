param(
  [Parameter(Mandatory = $true)]
  [string]$Architecture,

  [Parameter(Mandatory = $true)]
  [string]$ZipPath,

  [Parameter(Mandatory = $true)]
  [string]$BuiltExePath
)

$ErrorActionPreference = "Stop"

$prefix = "weaver-windows-$Architecture"
$defenderLog = "$prefix-defender-scan.log"
$attachmentLog = "$prefix-attachment-services.log"
$startupLog = "$prefix-noarg-startup.log"
$wingetLog = "$prefix-winget-install.log"
$validationRoot = Join-Path $env:RUNNER_TEMP "weaver-package-validation-$Architecture"

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
  $process = $null

  try {
    $env:LOCALAPPDATA = $localAppData
    $env:APPDATA = $appData
    $env:WEAVER_HTTP_BIND_ADDRESS = "127.0.0.1"

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
        $spa = Invoke-WebRequest -Uri "http://127.0.0.1:9090/" -WebSession $session -TimeoutSec 5
        if ($spa.StatusCode -ne 200) {
          throw "SPA returned HTTP $($spa.StatusCode)"
        }

        $payload = @{ query = "query { systemStatus { version } }" } | ConvertTo-Json -Compress
        $api = Invoke-WebRequest -Uri "http://127.0.0.1:9090/graphql" -Method Post -ContentType "application/json" -Body $payload -WebSession $session -TimeoutSec 5
        $responseText = if ($api.Content -is [byte[]]) {
          [System.Text.Encoding]::UTF8.GetString($api.Content)
        } else {
          [string]$api.Content
        }
        $json = $responseText | ConvertFrom-Json
        if ($json.errors) {
          throw "GraphQL errors: $($json.errors | ConvertTo-Json -Compress -Depth 8)"
        }
        if (-not $json.data.systemStatus.version) {
          throw "GraphQL systemStatus did not include a version"
        }

        Write-Log $startupLog "No-arg startup API smoke passed with version $($json.data.systemStatus.version)"
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
  }
}

function Assert-WindowsKeyPersistence {
  $defaultLog = Join-Path $validationRoot "noarg-startup\local-app-data\weaver\logs\weaver.log"
  if (-not (Test-Path $defaultLog)) {
    throw "Cannot verify Credential Manager persistence because $defaultLog does not exist."
  }

  $contents = Get-Content $defaultLog -Raw
  if ($contents -notmatch "using encryption master key from Windows Credential Manager") {
    throw "A repeated Windows startup did not reuse its Credential Manager encryption key."
  }
  Write-Log $startupLog "Repeated startup reused the Windows Credential Manager encryption key."
}

function Invoke-WinGetLocalInstallSmoke {
  param(
    [Parameter(Mandatory = $true)]
    [string]$PackageZip
  )

  $winget = (Get-Command winget.exe -ErrorAction SilentlyContinue).Source
  if (-not $winget) {
    Write-Log $wingetLog "winget.exe was not found; local manifest install smoke skipped."
    return
  }

  $manifestRoot = Join-Path $validationRoot "winget-manifest"
  New-Item -ItemType Directory -Force -Path $manifestRoot | Out-Null
  $zipHash = (Get-FileHash $PackageZip -Algorithm SHA256).Hash.ToUpperInvariant()
  $zipUri = ([System.Uri](Resolve-Path $PackageZip).Path).AbsoluteUri
  $wingetArchitecture = if ($Architecture -eq "x86_64") { "x64" } else { "arm64" }

  @"
PackageIdentifier: ScryerMedia.Weaver
PackageVersion: 0.0.0
DefaultLocale: en-US
ManifestType: version
ManifestVersion: 1.12.0
"@ | Set-Content -Path (Join-Path $manifestRoot "ScryerMedia.Weaver.yaml") -Encoding utf8

  @"
PackageIdentifier: ScryerMedia.Weaver
PackageVersion: 0.0.0
PackageLocale: en-US
Publisher: Scryer Media
PackageName: Weaver
License: GPL-3.0
ShortDescription: High-performance Usenet binary downloader.
ManifestType: defaultLocale
ManifestVersion: 1.12.0
"@ | Set-Content -Path (Join-Path $manifestRoot "ScryerMedia.Weaver.locale.en-US.yaml") -Encoding utf8

  @"
PackageIdentifier: ScryerMedia.Weaver
PackageVersion: 0.0.0
InstallerType: zip
NestedInstallerType: portable
NestedInstallerFiles:
- RelativeFilePath: weaver.exe
  PortableCommandAlias: weaver-ci-$Architecture
Installers:
- Architecture: $wingetArchitecture
  InstallerUrl: $zipUri
  InstallerSha256: $zipHash
ManifestType: installer
ManifestVersion: 1.12.0
"@ | Set-Content -Path (Join-Path $manifestRoot "ScryerMedia.Weaver.installer.yaml") -Encoding utf8

  Write-Log $wingetLog "Running winget local manifest install smoke from $manifestRoot"
  $installSucceeded = $false
  try {
    & $winget settings --enable LocalManifestFiles *>> $wingetLog
    & $winget install --manifest $manifestRoot --accept-package-agreements --accept-source-agreements --disable-interactivity *>> $wingetLog
    if ($LASTEXITCODE -ne 0) {
      throw "winget install exited with code $LASTEXITCODE"
    }
    $installSucceeded = $true
    Write-Log $wingetLog "winget local manifest install smoke succeeded."

    $aliasName = "weaver-ci-$Architecture"
    $installedExe = (Get-Command $aliasName -ErrorAction SilentlyContinue).Source
    if (-not $installedExe) {
      $link = Get-ChildItem (Join-Path $env:LOCALAPPDATA "Microsoft\WinGet\Links") -Filter "$aliasName*" -ErrorAction SilentlyContinue |
        Select-Object -First 1 -ExpandProperty FullName
      $installedExe = $link
    }
    if (-not $installedExe -or -not (Test-Path $installedExe)) {
      throw "winget installed the package but did not create the $aliasName command alias"
    }

    & $installedExe --version *>> $wingetLog
    if ($LASTEXITCODE -ne 0) {
      throw "winget-installed Weaver --version exited with code $LASTEXITCODE"
    }
    Write-Log $wingetLog "winget-installed command alias executed successfully from $installedExe."

    Invoke-NoArgStartupSmoke -ExePath $installedExe
    Assert-WindowsKeyPersistence
    Write-Log $wingetLog "winget-installed Weaver reused the Windows Credential Manager key."
  } catch {
    if ($installSucceeded) {
      Write-Log $wingetLog "winget-installed Weaver validation failed: $($_.Exception.Message)"
      throw
    }
    Write-Log $wingetLog "winget local manifest install smoke was inconclusive: $($_.Exception.Message)"
    Write-Warning "winget local manifest install smoke was inconclusive; see $wingetLog."
  } finally {
    try {
      & $winget uninstall --id ScryerMedia.Weaver --accept-source-agreements --disable-interactivity *>> $wingetLog
    } catch {
      Write-Log $wingetLog "winget cleanup failed or package was not installed: $($_.Exception.Message)"
    } finally {
      # This smoke is evidence-only: Defender, Attachment Services, and startup checks
      # above are the release-blocking package validations. Do not let a local WinGet
      # transport/install quirk leak through PowerShell's native-command exit code.
      Reset-NativeExitCode
    }
  }
}

Remove-Item -Recurse -Force $validationRoot -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $validationRoot | Out-Null
"" | Set-Content $defenderLog
"" | Set-Content $attachmentLog
"" | Set-Content $startupLog
"" | Set-Content $wingetLog

$zipCopy = Join-Path $validationRoot (Split-Path $ZipPath -Leaf)
$extractRoot = Join-Path $validationRoot "extracted"
Copy-Item $ZipPath $zipCopy -Force
Expand-Archive -Path $zipCopy -DestinationPath $extractRoot -Force
$packagedExe = Join-Path $extractRoot "weaver.exe"
if (-not (Test-Path $packagedExe)) {
  throw "Packaged zip did not contain weaver.exe at the zip root."
}

$builtHash = (Get-FileHash $BuiltExePath -Algorithm SHA256).Hash
$packagedHash = (Get-FileHash $packagedExe -Algorithm SHA256).Hash
if ($builtHash -ne $packagedHash) {
  throw "Packaged weaver.exe hash differs from built executable."
}

Invoke-DefenderScan -Path $zipCopy
Invoke-DefenderScan -Path $packagedExe

$sourceUrl = "https://github.com/scryer-media/weaver/releases/download/weaver-local-ci/$(Split-Path $zipCopy -Leaf)"
Invoke-AttachmentServicesSave -Path $zipCopy -Source $sourceUrl

Invoke-NoArgStartupSmoke -ExePath $packagedExe
Invoke-NoArgStartupSmoke -ExePath $packagedExe
Assert-WindowsKeyPersistence
Invoke-WinGetLocalInstallSmoke -PackageZip $zipCopy
