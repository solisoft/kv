#!/bin/sh
# SoliKV installer — works with any POSIX shell
# Usage: curl -sSL https://raw.githubusercontent.com/solisoft/kv/main/install.sh | sh
#   or:  sh install.sh [--system]

set -e

REPO="solisoft/kv"
INSTALL_DIR="$HOME/.local/bin"
SYSTEM_INSTALL=0

for arg in "$@"; do
  case "$arg" in
    --system) SYSTEM_INSTALL=1; INSTALL_DIR="/usr/local/bin" ;;
    --help|-h)
      echo "Usage: install.sh [--system]"
      echo "  --system  Install to /usr/local/bin (requires sudo)"
      echo "  Default:  Install to ~/.local/bin"
      exit 0
      ;;
    *) echo "Unknown option: $arg"; exit 1 ;;
  esac
done

# --- Detect OS ---
OS="$(uname -s)"
case "$OS" in
  Linux*)  OS="linux" ;;
  Darwin*) OS="darwin" ;;
  *) echo "Error: unsupported operating system: $OS"; exit 1 ;;
esac

# --- Detect architecture ---
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64)   ARCH="amd64" ;;
  aarch64|arm64)   ARCH="arm64" ;;
  *) echo "Error: unsupported architecture: $ARCH"; exit 1 ;;
esac

echo "Detected platform: ${OS}-${ARCH}"

# --- Pick a download tool ---
if command -v curl >/dev/null 2>&1; then
  fetch() { curl -fsSL "$1"; }
elif command -v wget >/dev/null 2>&1; then
  fetch() { wget -qO- "$1"; }
else
  echo "Error: curl or wget is required"; exit 1
fi

# --- Get latest version tag ---
API_URL="https://api.github.com/repos/${REPO}/releases/latest"
TAG=""
if TAG=$(fetch "$API_URL" 2>/dev/null | grep '"tag_name"' | head -1 | sed 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/'); then
  if [ -z "$TAG" ]; then
    TAG=""
  fi
fi

if [ -z "$TAG" ]; then
  echo "Error: could not fetch latest release tag from GitHub API." >&2
  echo "Please check your network connection and try again." >&2
  exit 1
fi

echo "Installing SoliKV ${TAG} ..."

# --- Download and verify ---
TARBALL="solikv-${OS}-${ARCH}.tar.gz"
CHECKSUMS_FILE="SHA256SUMS"
DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${TAG}/${TARBALL}"
CHECKSUMS_URL="https://github.com/${REPO}/releases/download/${TAG}/${CHECKSUMS_FILE}"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

echo "Downloading ${DOWNLOAD_URL} ..."
if ! fetch "$DOWNLOAD_URL" > "${TMP_DIR}/${TARBALL}"; then
  echo "Error: could not download ${TARBALL} from release ${TAG}." >&2
  echo "No build may be published for ${OS}-${ARCH}. See https://github.com/${REPO}/releases/tag/${TAG}" >&2
  exit 1
fi

echo "Downloading checksums ${CHECKSUMS_URL} ..."
if ! fetch "$CHECKSUMS_URL" > "${TMP_DIR}/${CHECKSUMS_FILE}"; then
  echo "Error: could not download ${CHECKSUMS_FILE} from release ${TAG}." >&2
  echo "The release is missing its checksum file, so the download cannot be verified." >&2
  echo "Please report this at https://github.com/${REPO}/issues" >&2
  exit 1
fi

# --- Verify checksum ---
# SHA256SUMS lists every release artifact (linux-amd64, darwin-arm64, ...).
# Filter to just our tarball so verification doesn't fail on missing files
# for other platforms. We require exactly one matching line.
echo "Verifying checksum ..."
cd "${TMP_DIR}"
EXPECTED=$(grep -E "[[:space:]]\*?${TARBALL}\$" "${CHECKSUMS_FILE}" || true)
if [ -z "$EXPECTED" ]; then
  echo "Error: ${TARBALL} not found in ${CHECKSUMS_FILE}" >&2
  exit 1
fi
echo "$EXPECTED" > "${CHECKSUMS_FILE}.filtered"
if command -v sha256sum >/dev/null 2>&1; then
  if ! sha256sum -c --strict "${CHECKSUMS_FILE}.filtered"; then
    echo "Error: checksum verification failed for ${TARBALL}" >&2
    exit 1
  fi
elif command -v shasum >/dev/null 2>&1; then
  if ! shasum -a 256 -c --strict "${CHECKSUMS_FILE}.filtered"; then
    echo "Error: checksum verification failed for ${TARBALL}" >&2
    exit 1
  fi
else
  echo "Error: sha256sum or shasum is required for checksum verification" >&2
  exit 1
fi
cd - >/dev/null
echo "Checksum verified."

# --- Extract with security options ---
echo "Extracting ..."
tar --no-same-owner -xzf "${TMP_DIR}/${TARBALL}" -C "$TMP_DIR"

# --- Install binary ---
if [ "$SYSTEM_INSTALL" = "1" ]; then
  echo "Installing to ${INSTALL_DIR} (may require sudo) ..."
  sudo install -m 755 "${TMP_DIR}/solikv" "${INSTALL_DIR}/solikv"
else
  mkdir -p "$INSTALL_DIR"
  install -m 755 "${TMP_DIR}/solikv" "${INSTALL_DIR}/solikv"
fi

# --- Check PATH ---
case ":${PATH}:" in
  *":${INSTALL_DIR}:"*) ;;
  *)
    echo ""
    echo "WARNING: ${INSTALL_DIR} is not in your PATH."
    echo "Add this to your shell profile (~/.bashrc, ~/.zshrc, etc.):"
    echo ""
    echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
    echo ""
    ;;
esac

# --- Verify ---
# Run the binary we just installed by absolute path. Checking `command -v solikv`
# instead would report success on an unrelated solikv already on PATH.
echo ""
if VERSION=$("${INSTALL_DIR}/solikv" --version 2>/dev/null); then
  echo "SoliKV installed successfully: ${VERSION} (${INSTALL_DIR}/solikv)"
elif "${INSTALL_DIR}/solikv" --help >/dev/null 2>&1; then
  # Releases up to and including v0.4.1 have no --version flag.
  echo "SoliKV ${TAG} installed successfully (${INSTALL_DIR}/solikv)"
else
  echo "SoliKV installed to ${INSTALL_DIR}/solikv, but running it failed." >&2
  echo "The download may be corrupt or built for a different platform." >&2
  exit 1
fi

# A different solikv earlier in PATH would shadow the one just installed.
RESOLVED=$(command -v solikv 2>/dev/null || true)
if [ -n "$RESOLVED" ] && [ "$RESOLVED" != "${INSTALL_DIR}/solikv" ]; then
  echo ""
  echo "NOTE: 'solikv' on your PATH resolves to ${RESOLVED}, not the copy just installed."
fi