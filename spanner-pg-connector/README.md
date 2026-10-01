# Spanner PG Connector (`spgc`)

`spanner-pg-connector` (aliased as `spgc`) is a CLI tool designed to make working with existing
PostgreSQL command-line tools (such as `psql` or `pg_dump`) against Spanner
easier. It bundles PGAdapter and a minimal Java runtime—so you do not need Java or Docker installed
locally—and automatically starts PGAdapter in the background on a dynamically assigned localhost
port, configures the standard PostgreSQL environment variables (`PGHOST` and `PGPORT`), runs your
PostgreSQL tool against it, and stops PGAdapter when the tool exits.

The PostgreSQL client tool itself (for example `psql`) is not bundled and must be installed and
available on your `PATH`.

## Installation

### Linux (`x86_64`) and macOS (`aarch64` / `x86_64`)

```shell
curl -fsSL https://raw.githubusercontent.com/GoogleCloudPlatform/pgadapter/postgresql-dialect/spanner-pg-connector/install.sh | sh
```

### Windows (`x86_64`, PowerShell)

```powershell
irm https://raw.githubusercontent.com/GoogleCloudPlatform/pgadapter/postgresql-dialect/spanner-pg-connector/install.ps1 | iex
```

By default, the installer downloads the latest release into `~/.spanner-pg-connector` and adds it
to your `PATH`. You can customize the installation with `VERSION` and `INSTALL_DIR`:

<!--- {x-version-update-start:google-cloud-spanner-pgadapter:released} -->
```shell
curl -fsSL https://raw.githubusercontent.com/GoogleCloudPlatform/pgadapter/postgresql-dialect/spanner-pg-connector/install.sh \
  | VERSION=v0.56.0 INSTALL_DIR="$HOME/.spanner-pg-connector" sh
```

```powershell
$env:VERSION = "v0.56.0"; $env:INSTALL_DIR = "$HOME\.spanner-pg-connector"; irm https://raw.githubusercontent.com/GoogleCloudPlatform/pgadapter/postgresql-dialect/spanner-pg-connector/install.ps1 | iex
```
<!--- {x-version-update-end} -->

### Manual Installation (Without the Installer Script)

You can also download and extract the release archive directly and add the extracted directory to
your `PATH` (both `spanner-pg-connector` and `spgc` are included in the archive):

#### Linux and macOS

<!--- {x-version-update-start:google-cloud-spanner-pgadapter:released} -->
```shell
VERSION=v0.56.0
PLATFORM=linux-x64 # linux-x64, mac-aarch64, or mac-x64
mkdir -p ~/.spanner-pg-connector
curl -fsSL "https://artifactregistry.googleapis.com/v1/projects/cloud-spanner-pg-adapter/locations/us/repositories/spanner-pg-connector/files/spanner-pg-connector:${VERSION}:spanner-pg-connector-${PLATFORM}.tar.gz:download?alt=media" \
  | tar -xz -C ~/.spanner-pg-connector
export PATH="$HOME/.spanner-pg-connector:$PATH"
```
<!--- {x-version-update-end} -->

#### Windows (PowerShell)

<!--- {x-version-update-start:google-cloud-spanner-pgadapter:released} -->
```powershell
$Version = "v0.56.0"
$InstallDir = Join-Path $HOME ".spanner-pg-connector"
$ZipPath = Join-Path $env:TEMP "spanner-pg-connector-windows-x64.zip"
New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
Invoke-RestMethod -Uri "https://artifactregistry.googleapis.com/v1/projects/cloud-spanner-pg-adapter/locations/us/repositories/spanner-pg-connector/files/spanner-pg-connector:${Version}:spanner-pg-connector-windows-x64.zip:download?alt=media" -OutFile $ZipPath
Expand-Archive -Path $ZipPath -DestinationPath $InstallDir -Force
$env:PATH = "$env:PATH;$InstallDir"
```
<!--- {x-version-update-end} -->

To make the `PATH` change persistent across new terminal sessions, add it to your shell profile
(for example `~/.bashrc` or `~/.zshrc` on Linux/macOS) or your user `Path` environment variable on
Windows.

Verify the installation by checking the version:

```shell
spgc --version
```

## Authentication

`spgc` does not require a PostgreSQL username or password between the client tool and the local
PGAdapter proxy. To authenticate with Spanner, `spgc` uses Google Cloud
**Application Default Credentials (ADC)** in the following order:

1. **`GOOGLE_APPLICATION_CREDENTIALS` environment variable**: Pointing to a service account key or
   credentials JSON file.
2. **User credentials via `gcloud`**: Set up by running:
   ```shell
   gcloud auth application-default login
   ```
3. **Attached service account**: Used automatically when running on Google Cloud environments such
   as Compute Engine, Cloud Shell, Cloud Workstations, Cloud Run, or GKE Workload Identity.

When connecting to the Spanner Emulator (`SPANNER_EMULATOR_HOST`), authentication is not
required and is disabled automatically.

## Usage

Pass the client command and its arguments directly to `spgc` (or `spanner-pg-connector`):

```shell
spgc psql -d "projects/my-project/instances/my-instance/databases/my-database"
```

You can also run `spgc --help` to view usage details or `spgc --version` to print the bundled
PGAdapter version.

### Environment Variables

You can configure the project, instance, default database, or Spanner Emulator via
environment variables:

* `GOOGLE_CLOUD_PROJECT`: Google Cloud project ID.
* `SPANNER_INSTANCE`: Spanner instance ID.
* `SPANNER_DATABASE`: Default Spanner database ID (used if the command does not specify `-d`, `--dbname`, or `--database`).
* `SPANNER_EMULATOR_HOST`: If set, connects to the Spanner Emulator at `host:port` (for example `localhost:9010`) instead of Spanner. Must be unset when connecting to Spanner.
* `GOOGLE_APPLICATION_CREDENTIALS`: Optional path to a Google Cloud credentials JSON file.

### Examples

```shell
export GOOGLE_CLOUD_PROJECT=my-project
export SPANNER_INSTANCE=my-instance

# Start an interactive psql session
spgc psql -d my-database

# Run a single query
spgc psql -d my-database -c "SELECT 1"

# Connect to the Spanner Emulator
SPANNER_EMULATOR_HOST=localhost:9010 spgc psql -d test-database
```
