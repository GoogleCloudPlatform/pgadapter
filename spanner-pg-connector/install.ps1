# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# install.ps1
# PowerShell script to install Spanner PG Connector on Windows

$Project = if ($env:PROJECT_ID) { $env:PROJECT_ID } else { "cloud-spanner-pg-adapter" }
$Location = if ($env:AR_LOCATION) { $env:AR_LOCATION } else { "us-central1" }
$Repository = if ($env:AR_REPOSITORY) { $env:AR_REPOSITORY } else { "spanner-pg-connector" }
$Package = "spanner-pg-connector"
$Version = $env:VERSION
$InstallDir = if ($env:INSTALL_DIR) { $env:INSTALL_DIR } else { (Join-Path $HOME ".spanner-pg-connector") }

$Token = if ($env:TOKEN) { $env:TOKEN } else { $null }

# Opportunistically fetch gcloud token if gcloud is installed and token was not provided
if (-not $Token) {
    if (Get-Command gcloud -ErrorAction SilentlyContinue) {
        $Token = (gcloud auth print-access-token 2>$null)
    }
}

$Headers = @{}
if ($Token) {
    $Headers["Authorization"] = "Bearer $Token"
}

# Resolve the latest version if not explicitly specified
if ([string]::IsNullOrEmpty($Version)) {
    Write-Host "Querying Artifact Registry for the latest release version..."
    $Uri = "https://artifactregistry.googleapis.com/v1/projects/$Project/locations/$Location/repositories/$Repository/packages/spanner-pg-connector/versions"
    try {
        $Result = Invoke-RestMethod -Uri $Uri -Headers $Headers -ErrorAction Stop
        $Version = $Result.versions.name | ForEach-Object { Split-Path $_ -Leaf } | Where-Object { $_ -match '^\d+\.\d+\.\d+$' } | Sort-Object {[version]$_} | Select-Object -Last 1
    } catch {
        # Fallback to gcloud if available
        if (Get-Command gcloud -ErrorAction SilentlyContinue) {
            $VersionsList = gcloud artifacts versions list --package=spanner-pg-connector --project=$Project --location=$Location --repository=$Repository --format="value(name)" 2>$null
            $Version = $VersionsList | Where-Object { $_ -match '^\d+\.\d+\.\d+$' } | Sort-Object {[version]$_} | Select-Object -Last 1
        }
    }

    if ([string]::IsNullOrEmpty($Version)) {
        Write-Error "Error: Could not resolve the latest version from Artifact Registry."
        Write-Host "If the repository is private, please set `$env:TOKEN or run 'gcloud auth login'." -ForegroundColor Yellow
        Write-Host "Alternatively, pass `$env:VERSION explicitly." -ForegroundColor Yellow
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

# 2. Download package
Write-Host "Downloading package from Artifact Registry..."
$DownloadUrl = "https://artifactregistry.googleapis.com/v1/projects/$Project/locations/$Location/repositories/$Repository/files/spanner-pg-connector:$Version:spanner-pg-connector-windows-x64.zip:download?alt=media"
$ZipPath = Join-Path $env:TEMP "spanner-pg-connector-windows-x64.zip"

$DownloadSuccess = $false
try {
    Invoke-RestMethod -Uri $DownloadUrl -Headers $Headers -OutFile $ZipPath -ErrorAction Stop
    $DownloadSuccess = $true
} catch {
    if (Get-Command gcloud -ErrorAction SilentlyContinue) {
        Write-Host "Direct download failed, falling back to gcloud artifacts..." -ForegroundColor Yellow
        gcloud artifacts generic download --project=$Project --location=$Location --repository=$Repository --package="spanner-pg-connector" --version="$Version" --name="spanner-pg-connector-windows-x64.zip" --destination="$env:TEMP"
        if (Test-Path $ZipPath) {
            $DownloadSuccess = $true
        }
    }
}

if (-not $DownloadSuccess) {
    Write-Error "Error: Failed to download spanner-pg-connector-windows-x64.zip from Artifact Registry."
    Write-Host "If the repository is private, please set `$env:TOKEN or run 'gcloud auth login'." -ForegroundColor Yellow
    Exit 1
}

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
