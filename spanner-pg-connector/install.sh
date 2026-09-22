#!/bin/sh
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

# Google Cloud Spanner PG Connector - One-line Installer
# Safely installs the PG Connector CLI bundle and optimizes its startup speed.

set -e

# Configuration (Supports env overrides)
VERSION="${VERSION:-}"
PROJECT_ID="${PROJECT_ID:-cloud-spanner-pg-adapter}"
AR_LOCATION="${AR_LOCATION:-us-central1}"
AR_REPOSITORY="${AR_REPOSITORY:-spanner-pg-connector}"
INSTALL_DIR="${INSTALL_DIR:-${HOME}/.spanner-pg-connector}"

# Resolve auth header opportunistically
AUTH_HEADER=""
TOKEN="${TOKEN:-}"
if [ -z "${TOKEN}" ] && command -v gcloud >/dev/null 2>&1; then
  TOKEN=$(gcloud auth print-access-token 2>/dev/null || true)
fi
if [ -n "${TOKEN}" ]; then
  AUTH_HEADER="Authorization: Bearer ${TOKEN}"
fi

# Resolve the latest version if not explicitly specified
if [ -z "${VERSION}" ]; then
  echo "Querying Artifact Registry for the latest release version..."
  VERSIONS_URL="https://artifactregistry.googleapis.com/v1/projects/${PROJECT_ID}/locations/${AR_LOCATION}/repositories/${AR_REPOSITORY}/packages/spanner-pg-connector/versions"

  if [ -n "${AUTH_HEADER}" ]; then
    RESPONSE=$(curl -s -L -H "${AUTH_HEADER}" "${VERSIONS_URL}" 2>/dev/null || true)
  else
    RESPONSE=$(curl -s -L "${VERSIONS_URL}" 2>/dev/null || true)
  fi

  # `sort -V` is GNU-only and silently yields nothing on macOS. The grep above guarantees three
  # numeric fields, so sort them as numbers instead.
  VERSION=$(echo "${RESPONSE}" \
    | grep -oE '"name": "[^"]+"' \
    | cut -d'"' -f4 \
    | awk -F/ '{print $NF}' \
    | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' \
    | sort -t. -k 1,1n -k 2,2n -k 3,3n \
    | tail -n 1)

  # If curl didn't find any version and gcloud is installed, try gcloud as fallback
  if [ -z "${VERSION}" ] && command -v gcloud >/dev/null 2>&1; then
    VERSION=$(gcloud artifacts versions list \
      --package=spanner-pg-connector \
      --project="${PROJECT_ID}" \
      --location="${AR_LOCATION}" \
      --repository="${AR_REPOSITORY}" \
      --format="value(name)" 2>/dev/null \
      | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' \
      | sort -t. -k 1,1n -k 2,2n -k 3,3n \
      | tail -n 1 || true)
  fi

  if [ -z "${VERSION}" ]; then
    echo "Error: Could not resolve the latest version from Artifact Registry." >&2
    if echo "${RESPONSE}" | grep -qiE 'unauthenticated|permission denied|PERMISSION_DENIED|UNAUTHENTICATED'; then
      echo "The repository may be private. Please authenticate via 'gcloud auth login' or pass TOKEN=\"<token>\"." >&2
    fi
    echo "Alternatively, you can specify the version explicitly: VERSION=1.0.0 ./install.sh" >&2
    exit 1
  fi
  echo "Resolved latest version: ${VERSION}"
fi

echo "Installing Spanner PG Connector..."

# 1. Detect OS and CPU Architecture
OS="$(uname -s)"
ARCH="$(uname -m)"

case "${OS}" in
  Darwin)
    OS_NAME="mac"
    ;;
  Linux)
    OS_NAME="linux"
    ;;
  *)
    echo "Error: Unsupported Operating System: ${OS}" >&2
    exit 1
    ;;
esac

case "${ARCH}" in
  x86_64)
    ARCH_NAME="x64"
    ;;
  arm64|aarch64)
    if [ "${OS_NAME}" = "linux" ]; then
      echo "Error: Linux ARM64 support is currently experimental." >&2
      exit 1
    fi
    ARCH_NAME="aarch64"
    ;;
  *)
    echo "Error: Unsupported CPU Architecture: ${ARCH}" >&2
    exit 1
    ;;
esac

echo "Detected Platform: ${OS_NAME}-${ARCH_NAME}"

# 2. Define package names
PACKAGE_NAME="spanner-pg-connector-${OS_NAME}-${ARCH_NAME}.tar.gz"

# 3. Download the release bundle into a staging directory. The installation that is already there
# is only touched once the new payload is on disk and complete, so that a failed download or a
# corrupt archive leaves the existing installation working.
STAGING_DIR="${INSTALL_DIR}.staging.$$"
trap 'rm -rf "${STAGING_DIR}"' EXIT
rm -rf "${STAGING_DIR}"
mkdir -p "${STAGING_DIR}"

KEPT_MESSAGE="The existing installation in ${INSTALL_DIR} has been left unchanged."
DOWNLOAD_URL="https://artifactregistry.googleapis.com/v1/projects/${PROJECT_ID}/locations/${AR_LOCATION}/repositories/${AR_REPOSITORY}/files/spanner-pg-connector:${VERSION}:${PACKAGE_NAME}:download?alt=media"
ARCHIVE="${STAGING_DIR}/${PACKAGE_NAME}"

DOWNLOAD_SUCCESS=false
if command -v curl >/dev/null 2>&1; then
  echo "Downloading package from Artifact Registry..."

  if [ -n "${AUTH_HEADER}" ]; then
    HTTP_STATUS=$(curl -s -L -H "${AUTH_HEADER}" -w "%{http_code}" -o "${ARCHIVE}" "${DOWNLOAD_URL}" 2>/dev/null || true)
  else
    HTTP_STATUS=$(curl -s -L -w "%{http_code}" -o "${ARCHIVE}" "${DOWNLOAD_URL}" 2>/dev/null || true)
  fi

  if [ "${HTTP_STATUS}" = "200" ] && [ -s "${ARCHIVE}" ]; then
    DOWNLOAD_SUCCESS=true
  else
    rm -f "${ARCHIVE}"
  fi
fi

if [ "${DOWNLOAD_SUCCESS}" != "true" ] && command -v gcloud >/dev/null 2>&1; then
  echo "Direct download not available or failed; falling back to gcloud artifacts..."
  # Guarded by `if`, because a bare call would abort the installer under `set -e` and skip the
  # diagnostics below.
  if gcloud artifacts generic download \
    --project="${PROJECT_ID}" \
    --location="${AR_LOCATION}" \
    --repository="${AR_REPOSITORY}" \
    --package="spanner-pg-connector" \
    --version="${VERSION}" \
    --name="${PACKAGE_NAME}" \
    --destination="${STAGING_DIR}" && [ -s "${ARCHIVE}" ]; then
    DOWNLOAD_SUCCESS=true
  fi
fi

if [ "${DOWNLOAD_SUCCESS}" != "true" ]; then
  echo "Error: Failed to download ${PACKAGE_NAME} version ${VERSION} from Artifact Registry." >&2
  echo "If the repository is private, please set TOKEN=\"<token>\" or install and authenticate 'gcloud'." >&2
  echo "${KEPT_MESSAGE}" >&2
  exit 1
fi

if ! tar -xzf "${ARCHIVE}" -C "${STAGING_DIR}"; then
  echo "Error: Failed to extract ${PACKAGE_NAME}. The download may be corrupt." >&2
  echo "${KEPT_MESSAGE}" >&2
  exit 1
fi
rm -f "${ARCHIVE}"

# A truncated archive can extract without error, so check that the payload is actually usable
# before the working installation is replaced with it.
for required in spanner-pg-connector pgadapter.jar; do
  if [ ! -e "${STAGING_DIR}/${required}" ]; then
    echo "Error: The downloaded package is incomplete, '${required}' is missing." >&2
    echo "${KEPT_MESSAGE}" >&2
    exit 1
  fi
done

# 4. Swap the staged payload in. The previous payload is removed entry by entry rather than the
# whole directory, otherwise its jars stay in lib/ and the launcher's lib/* classpath loads two
# versions of the same dependency. INSTALL_DIR itself is left alone, so a shell sitting in it does
# not block the upgrade and unmanaged files survive.
mkdir -p "${INSTALL_DIR}"
rm -f "${INSTALL_DIR}/pgadapter.jsa" "${INSTALL_DIR}/install_path.txt"
for managed in lib custom-jre pgadapter.jar spanner-pg-connector spgc; do
  rm -rf "${INSTALL_DIR:?}/${managed}"
  if [ -e "${STAGING_DIR}/${managed}" ]; then
    mv "${STAGING_DIR}/${managed}" "${INSTALL_DIR}/"
  fi
done

# Ensure launcher is executable and create alias symlink
chmod +x "${INSTALL_DIR}/spanner-pg-connector"
ln -sf spanner-pg-connector "${INSTALL_DIR}/spgc"

echo "Extracted files to ${INSTALL_DIR}"

# 5. Add to Shell Profile PATH
SHELL_CONFIG=""
case "${SHELL}" in
  */zsh)
    SHELL_CONFIG="${HOME}/.zshrc"
    ;;
  */bash)
    if [ -f "${HOME}/.bash_profile" ]; then
      SHELL_CONFIG="${HOME}/.bash_profile"
    else
      SHELL_CONFIG="${HOME}/.bashrc"
    fi
    ;;
  *)
    if [ -f "${HOME}/.profile" ]; then
      SHELL_CONFIG="${HOME}/.profile"
    fi
    ;;
esac

PATH_EXPORT="export PATH=\"\${PATH}:${INSTALL_DIR}\""

# The PATH entry lives in a delimited block that is rewritten in place. Appending was not
# idempotent: it duplicated entries and left old install directories on PATH forever.
BEGIN_MARKER="# >>> spanner-pg-connector >>>"
END_MARKER="# <<< spanner-pg-connector <<<"

# Leaves exactly one managed block at the end of SHELL_CONFIG, also removing the un-delimited
# entries written by earlier versions of this installer. Returns 2 if no change was needed.
write_managed_block() {
  config="$1"
  tmp="${config}.spgc-tmp.$$"

  if [ -f "${config}" ]; then
    awk -v begin_marker="${BEGIN_MARKER}" -v end_marker="${END_MARKER}" '
      # Drop any existing managed block.
      $0 == begin_marker { in_block = 1; next }
      $0 == end_marker   { in_block = 0; next }
      in_block { next }

      # Legacy, un-delimited entries.
      /^# Spanner PG (Connector|Starter) CLI path mapping$/ { after_legacy_header = 1; next }
      after_legacy_header && /^export PATH=.*(spanner-pg-connector|spanner-pg-starter)/ {
        after_legacy_header = 0
        next
      }
      { after_legacy_header = 0; print }
    ' "${config}" > "${tmp}" || { rm -f "${tmp}"; return 1; }
  else
    : > "${tmp}"
  fi

  # Collapse any run of blank lines that removals may have left at the end of the file.
  awk 'BEGIN { blanks = 0 }
       /^[[:space:]]*$/ { blanks++; next }
       { while (blanks-- > 0) print ""; blanks = 0; print }
      ' "${tmp}" > "${tmp}.trimmed" && mv "${tmp}.trimmed" "${tmp}"

  {
    printf "\n%s\n" "${BEGIN_MARKER}"
    printf "%s\n" "${PATH_EXPORT}"
    printf "%s\n" "${END_MARKER}"
  } >> "${tmp}"

  # A no-op run must not touch the file, so that it does not churn the user's dotfiles or backups.
  if [ -f "${config}" ] && cmp -s "${tmp}" "${config}"; then
    rm -f "${tmp}"
    return 2
  fi

  if [ -f "${config}" ]; then
    cp "${config}" "${config}.spanner-pg-connector.bak"
  fi
  mv "${tmp}" "${config}"
  return 0
}

if [ -n "${SHELL_CONFIG}" ]; then
  # Capture the status conditionally; a bare call would abort the installer under `set -e`.
  write_managed_block "${SHELL_CONFIG}" && block_status=0 || block_status=$?
  case "${block_status}" in
    0)
      echo "Configured installation path in ${SHELL_CONFIG}."
      if [ -f "${SHELL_CONFIG}.spanner-pg-connector.bak" ]; then
        echo "  (previous contents saved to ${SHELL_CONFIG}.spanner-pg-connector.bak)"
      fi
      ;;
    2)
      echo "Installation path already configured in ${SHELL_CONFIG}."
      ;;
    *)
      echo "Warning: could not update ${SHELL_CONFIG}. Please add this to your PATH manually:"
      echo "  ${PATH_EXPORT}"
      ;;
  esac
else
  echo "Warning: Could not automatically detect shell profile. Please add this manually to your PATH:"
  echo "  ${PATH_EXPORT}"
fi

printf "\n-----------------------------------------------------\n"
echo "Installation complete!"
if [ -n "${SHELL_CONFIG}" ]; then
  echo "Please reload your terminal session or run:"
  echo "  source ${SHELL_CONFIG}"
else
  echo "Please add ${INSTALL_DIR} to your PATH, then reload your terminal session."
fi
echo "To begin using: spgc psql"
echo "-----------------------------------------------------"
