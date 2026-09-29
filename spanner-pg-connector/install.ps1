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
        $Version = $Result.versions.name | ForEach-Object { Split-Path $_ -Leaf } | Where-Object { $_ -match '^v?\d+\.\d+\.\d+$' } | Sort-Object { [version]($_ -replace '^v', '') } | Select-Object -Last 1
    } catch {
        # Fallback to gcloud if available
        if (Get-Command gcloud -ErrorAction SilentlyContinue) {
            $VersionsList = gcloud artifacts versions list --package=spanner-pg-connector --project=$Project --location=$Location --repository=$Repository --format="value(name)" 2>$null
            $Version = $VersionsList | Where-Object { $_ -match '^v?\d+\.\d+\.\d+$' } | Sort-Object { [version]($_ -replace '^v', '') } | Select-Object -Last 1
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

# 1. Download and extract into a staging directory. The installation that is already there is only
# touched once the new payload is on disk and complete, so that a failed download or a corrupt
# archive leaves the existing installation working.
$StagingDir = Join-Path $env:TEMP "spanner-pg-connector-staging-$PID"
$ZipPath = Join-Path $env:TEMP "spanner-pg-connector-windows-x64-$PID.zip"
$KeptMessage = "The existing installation in $InstallDir has been left unchanged."

Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $StagingDir
Remove-Item -Force -ErrorAction SilentlyContinue $ZipPath
New-Item -ItemType Directory -Path $StagingDir -Force | Out-Null

try {
    # 2. Download package
    Write-Host "Downloading package from Artifact Registry..."
    $DownloadUrl = "https://artifactregistry.googleapis.com/v1/projects/$Project/locations/$Location/repositories/$Repository/files/spanner-pg-connector:$Version:spanner-pg-connector-windows-x64.zip:download?alt=media"

    $DownloadSuccess = $false
    try {
        Invoke-RestMethod -Uri $DownloadUrl -Headers $Headers -OutFile $ZipPath -ErrorAction Stop
        $DownloadSuccess = $true
    } catch {
        if (Get-Command gcloud -ErrorAction SilentlyContinue) {
            Write-Host "Direct download failed, falling back to gcloud artifacts..." -ForegroundColor Yellow
            gcloud artifacts generic download --project=$Project --location=$Location --repository=$Repository --package="spanner-pg-connector" --version="$Version" --name="spanner-pg-connector-windows-x64.zip" --destination="$StagingDir"
            $Downloaded = Join-Path $StagingDir "spanner-pg-connector-windows-x64.zip"
            if ($LASTEXITCODE -eq 0 -and (Test-Path $Downloaded)) {
                Move-Item -Force $Downloaded $ZipPath
                $DownloadSuccess = $true
            }
        }
    }

    if (-not $DownloadSuccess) {
        Write-Error "Error: Failed to download spanner-pg-connector-windows-x64.zip version $Version from Artifact Registry."
        Write-Host "If the repository is private, please set `$env:TOKEN or run 'gcloud auth login'." -ForegroundColor Yellow
        Write-Host $KeptMessage -ForegroundColor Yellow
        Exit 1
    }

    # 3. Extract into staging
    Write-Host "Extracting files..."
    try {
        Expand-Archive -Path $ZipPath -DestinationPath $StagingDir -Force -ErrorAction Stop
    } catch {
        Write-Error "Error: Failed to extract the downloaded archive. It may be corrupt. $_"
        Write-Host $KeptMessage -ForegroundColor Yellow
        Exit 1
    }

    # A truncated archive can extract without error, so check that the payload is actually usable
    # before the working installation is replaced with it.
    foreach ($required in @("spanner-pg-connector.cmd", "pgadapter.jar")) {
        if (-not (Test-Path (Join-Path $StagingDir $required))) {
            Write-Error "Error: The downloaded package is incomplete, '$required' is missing."
            Write-Host $KeptMessage -ForegroundColor Yellow
            Exit 1
        }
    }

    # 4. Swap the staged payload in. Only managed assets are replaced, so user files survive and a
    # directory that some shell is sitting in does not block the upgrade.
    Write-Host "Installing to $InstallDir..."
    if (-not (Test-Path $InstallDir)) {
        New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    }
    foreach ($managed in @("lib", "custom-jre", "pgadapter.jar", "spanner-pg-connector.cmd", "spgc.cmd")) {
        $target = Join-Path $InstallDir $managed
        if (Test-Path $target) { Remove-Item -Recurse -Force $target }
        $staged = Join-Path $StagingDir $managed
        if (Test-Path $staged) { Move-Item -Force $staged $target }
    }
} finally {
    Remove-Item -Force -ErrorAction SilentlyContinue $ZipPath
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $StagingDir
}

# 5. Add to user PATH environment variable
# Rebuild the entry rather than only appending, so that an older install directory does not stay
# on PATH and win over this one.
Write-Host "Configuring environment PATH..."
$UserPath = [Environment]::GetEnvironmentVariable("Path", "User")
$Entries = @()
if ($UserPath) {
    $Entries = $UserPath -split ";"
}

# Drop empty entries, this install dir, and the default locations used by earlier runs of this
# installer and of its predecessor. Only directories that this installer can itself have created
# are removed, so an unrelated directory that merely contains the product name in its path (a
# source checkout, for example) keeps its place on PATH.
$ManagedDirs = @(
    $InstallDir
    (Join-Path $HOME ".spanner-pg-connector")
    (Join-Path $HOME ".spanner-pg-starter")
) | ForEach-Object { $_.TrimEnd("\") }

$Cleaned = $Entries | Where-Object {
    $_ -and ($ManagedDirs -notcontains $_.TrimEnd("\"))
}

$Removed = $Entries.Count - $Cleaned.Count
if ($Removed -gt 0) {
    Write-Host "Removed $Removed stale Spanner PG Connector entry/entries from user PATH." -ForegroundColor Yellow
}

# Filter blanks: @($null) is a one-element array, and an empty PATH entry is interpreted by
# Windows as the current directory.
$NewPath = (@($Cleaned) + $InstallDir | Where-Object { $_ }) -join ";"
if ($NewPath -ne $UserPath) {
    [Environment]::SetEnvironmentVariable("Path", $NewPath, "User")
    Write-Host "Added $InstallDir to user PATH." -ForegroundColor Cyan
} else {
    Write-Host "User PATH already up to date." -ForegroundColor Cyan
}
if (($env:PATH -split ";") -notcontains $InstallDir) {
    $env:PATH = "$env:PATH;$InstallDir"
}
Write-Host "-----------------------------------------------------" -ForegroundColor Green
Write-Host "Installation complete!" -ForegroundColor Green
Write-Host "Please restart your PowerShell session or environment to begin using: spgc psql"
Write-Host "-----------------------------------------------------"
