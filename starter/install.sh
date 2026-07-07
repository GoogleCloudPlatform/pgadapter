#!/bin/sh

# Google Cloud Spanner PG Connector - One-line Installer
# Safely installs the PG Connector CLI bundle and optimizes its startup speed.

set -e

# Configuration (Supports env overrides)
VERSION="${VERSION:-}"
PROJECT_ID="${PROJECT_ID:-span-cloud-testing}"
AR_LOCATION="${AR_LOCATION:-us-central1}"
AR_REPOSITORY="${AR_REPOSITORY:-spanner-pg-connector}"
INSTALL_DIR="${INSTALL_DIR:-${HOME}/.spanner-pg-connector}"

# Resolve the latest version if not explicitly specified
if [ -z "${VERSION}" ]; then
  echo "Querying Artifact Registry for the latest release version..."
  if [ -n "${TOKEN}" ] || [ -n "${ACCESS_TOKEN}" ]; then
    auth_token="${TOKEN:-$ACCESS_TOKEN}"
    VERSION=$(curl -s -L -H "Authorization: Bearer ${auth_token}" \
      "https://artifactregistry.googleapis.com/v1/projects/${PROJECT_ID}/locations/${AR_LOCATION}/repositories/${AR_REPOSITORY}/packages/spanner-pg-connector/versions" \
      | grep -oE '"name": "[^"]+"' \
      | cut -d'"' -f4 \
      | awk -F/ '{print $NF}' \
      | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' \
      | sort -V \
      | tail -n 1)
  else
    VERSION=$(gcloud artifacts versions list \
      --package=spanner-pg-connector \
      --project="${PROJECT_ID}" \
      --location="${AR_LOCATION}" \
      --repository="${AR_REPOSITORY}" \
      --format="value(name)" 2>/dev/null \
      | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' \
      | sort -V \
      | tail -n 1)
  fi

  if [ -z "${VERSION}" ]; then
    echo "Error: Could not resolve the latest version from Artifact Registry."
    echo "Please ensure you have authenticated with 'gcloud auth application-default login' or pass the VERSION environment variable explicitly."
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

# Setup install directory
mkdir -p "${INSTALL_DIR}"
rm -f "${INSTALL_DIR}/pgadapter.jsa" "${INSTALL_DIR}/install_path.txt"

# 3. Download and Extract Release Bundle
if [ -f "./target/dist/${PACKAGE_NAME}" ]; then
  echo "Found local release package. Installing locally..."
  tar -xzf "./target/dist/${PACKAGE_NAME}" -C "${INSTALL_DIR}"
elif [ -f "./${PACKAGE_NAME}" ]; then
  echo "Found local package in root. Installing locally..."
  tar -xzf "./${PACKAGE_NAME}" -C "${INSTALL_DIR}"
else
  # Download from Generic Artifact Registry
  # Prioritize curl if a TOKEN is explicitly provided in the environment
  if [ -n "${TOKEN}" ] || [ -n "${ACCESS_TOKEN}" ] && command -v curl >/dev/null 2>&1; then
    echo "Downloading package via curl from Generic Registry REST API..."
    
    # Inject token headers if present in environment
    AUTH_HEADER=""
    if [ -n "${TOKEN}" ]; then
      AUTH_HEADER="Authorization: Bearer ${TOKEN}"
    elif [ -n "${ACCESS_TOKEN}" ]; then
      AUTH_HEADER="Authorization: Bearer ${ACCESS_TOKEN}"
    fi
    
    DOWNLOAD_URL="https://artifactregistry.googleapis.com/v1/projects/${PROJECT_ID}/locations/${AR_LOCATION}/repositories/${AR_REPOSITORY}/files/spanner-pg-connector:${VERSION}:${PACKAGE_NAME}:download?alt=media"
    
    if [ -n "${AUTH_HEADER}" ]; then
      curl -fsSL -H "${AUTH_HEADER}" "${DOWNLOAD_URL}" | tar -xzf - -C "${INSTALL_DIR}"
    else
      curl -fsSL "${DOWNLOAD_URL}" | tar -xzf - -C "${INSTALL_DIR}"
    fi
  elif command -v gcloud >/dev/null 2>&1; then
    echo "Downloading package via gcloud artifacts from Generic Registry..."
    gcloud artifacts generic download \
      --project="${PROJECT_ID}" \
      --location="${AR_LOCATION}" \
      --repository="${AR_REPOSITORY}" \
      --package="spanner-pg-connector" \
      --version="${VERSION}" \
      --name="${PACKAGE_NAME}" \
      --destination="${INSTALL_DIR}"
    
    tar -xzf "${INSTALL_DIR}/${PACKAGE_NAME}" -C "${INSTALL_DIR}"
    rm -f "${INSTALL_DIR}/${PACKAGE_NAME}"
  else
    echo "Error: gcloud or curl is required to download the package from Generic Artifact Registry." >&2
    exit 1
  fi
fi

# Ensure launcher is executable and create alias symlink
chmod +x "${INSTALL_DIR}/spanner-pg-connector"
ln -sf spanner-pg-connector "${INSTALL_DIR}/spgc"

echo "Extracted files to ${INSTALL_DIR}"

# 4. Add to Shell Profile PATH
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

if [ -n "${SHELL_CONFIG}" ]; then
  if [ -f "${SHELL_CONFIG}" ]; then
    if ! grep -q "${INSTALL_DIR}" "${SHELL_CONFIG}"; then
      printf "\n# Spanner PG Connector CLI path mapping\n" >> "${SHELL_CONFIG}"
      printf "%s\n" "${PATH_EXPORT}" >> "${SHELL_CONFIG}"
      echo "Added installation path to ${SHELL_CONFIG}."
    else
      echo "Installation path already configured in ${SHELL_CONFIG}."
    fi
  else
    printf "%s\n" "${PATH_EXPORT}" >> "${SHELL_CONFIG}"
    echo "Created and added path mapping to ${SHELL_CONFIG}."
  fi
else
  echo "Warning: Could not automatically detect shell profile. Please add this manually to your PATH:"
  echo "  ${PATH_EXPORT}"
fi
printf "\n-----------------------------------------------------\n"
echo "Installation complete!"
echo "Please reload your terminal session or run:"
echo "  source ${SHELL_CONFIG}"
echo "To begin using: spgc psql"
echo "-----------------------------------------------------"
