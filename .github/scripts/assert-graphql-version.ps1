[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [string]$BaseUrl
)

$ErrorActionPreference = "Stop"
$payload = @{ query = "query { version }" } | ConvertTo-Json -Compress

try {
  $response = Invoke-WebRequest `
    -Uri "$BaseUrl/graphql" `
    -Method Post `
    -ContentType "application/json" `
    -Body $payload `
    -TimeoutSec 10
} catch {
  throw "GraphQL version probe request failed: $($_.Exception.Message)"
}

if ($response.StatusCode -ne 200) {
  throw "GraphQL version probe returned HTTP $($response.StatusCode)"
}

try {
  $graphql = $response.Content | ConvertFrom-Json -ErrorAction Stop
} catch {
  throw "GraphQL version probe returned invalid JSON: $($response.Content)"
}

$errors = $graphql.PSObject.Properties["errors"]
if ($null -ne $errors -and $null -ne $errors.Value -and @($errors.Value).Count -gt 0) {
  throw "GraphQL version probe returned errors: $($errors.Value | ConvertTo-Json -Compress -Depth 5)"
}

$data = $graphql.PSObject.Properties["data"]
$version = if ($null -eq $data) { $null } else { $data.Value.PSObject.Properties["version"] }
if ($null -eq $version -or $version.Value -isnot [string] -or [string]::IsNullOrWhiteSpace($version.Value)) {
  throw "GraphQL version probe returned no non-empty string data.version"
}

Write-Host "Weaver GraphQL version smoke test passed: $($version.Value)"
