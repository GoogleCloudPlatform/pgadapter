# install.ps1
# PowerShell script to install Spanner PG Connector on Windows

$Project = "span-cloud-testing"
$Location = "us-central1"
$Repository = "spanner-pg-connector"
$Package = "spanner-pg-connector"
$Version = $env:VERSION
$InstallDir = Join-Path $HOME ".spanner-pg-connector"

# Resolve the latest version if not explicitly specified
if ([string]::IsNullOrEmpty($Version)) {
    Write-Host "Querying Artifact Registry for the latest release version..."
    if ($Token) {
        $Uri = "https://artifactregistry.googleapis.com/v1/projects/$Project/locations/$Location/repositories/$Repository/packages/spanner-pg-connector/versions"
        $Result = Invoke-RestMethod -Uri $Uri -Headers @{"Authorization" = "Bearer $Token"}
        $Version = $Result.versions.name | ForEach-Object { Split-Path $_ -Leaf } | Where-Object { $_ -match '^\d+\.\d+\.\d+$' } | Sort-Object {[version]$_} | Select-Object -Last 1
    } else {
        $VersionsList = gcloud artifacts versions list --package=spanner-pg-connector --project=$Project --location=$Location --repository=$Repository --format="value(name)"
        $Version = $VersionsList | Where-Object { $_ -match '^\d+\.\d+\.\d+$' } | Sort-Object {[version]$_} | Select-Object -Last 1
    }

    if ([string]::IsNullOrEmpty($Version)) {
        Write-Error "Error: Could not resolve the latest version from Artifact Registry."
        Exit 1
    }
    Write-Host "Resolved latest version: $Version" -ForegroundColor Cyan
}

Write-Host "Installing Spanner PG Connector..." -ForegroundColor Green

# 1. Clean previous installation
if (Test-Path $InstallDir) {
    Remove-Item -Recurse -Force $InstallDir
}
New-Item -ItemType Directory -Path $InstallDir | Out-Null

# 2. Authenticate and download package
Write-Host "Downloading package from Artifact Registry..."
$Token = gcloud auth application-default print-access-token
$Headers = @{
    "Authorization" = "Bearer $Token"
}
$DownloadUrl = "https://artifactregistry.googleapis.com/v1/projects/$Project/locations/$Location/repositories/$Repository/files/spanner-pg-connector:$Version:spanner-pg-connector-windows-x64.zip:download?alt=media"

$ZipPath = Join-Path $env:TEMP "spanner-pg-connector-windows-x64.zip"
Invoke-RestMethod -Uri $DownloadUrl -Headers $Headers -OutFile $ZipPath

# 3. Extract zip
Write-Host "Extracting files to $InstallDir..."
Expand-Archive -Path $ZipPath -DestinationPath $InstallDir -Force
Remove-Item $ZipPath

# 4. Add to user PATH environment variable if not already present
Write-Host "Configuring environment PATH..."
$UserPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($UserPath -split ";" -notcontains $InstallDir) {
    [Environment]::SetEnvironmentVariable("Path", "$UserPath;$InstallDir", "User")
    $env:PATH = "$env:PATH;$InstallDir"
    Write-Host "Added $InstallDir to user PATH." -ForegroundColor Cyan
}
Write-Host "-----------------------------------------------------" -ForegroundColor Green
Write-Host "Installation complete!" -ForegroundColor Green
Write-Host "Please restart your PowerShell session or environment to begin using: spgc psql"
Write-Host "-----------------------------------------------------"
