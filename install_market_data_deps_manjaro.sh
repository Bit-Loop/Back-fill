#!/usr/bin/env bash

# ChronoX - Market Data Dependencies Installer for Manjaro / Arch Linux
# Converts scripts/install_market_data_deps.py into a Manjaro-friendly bash installer.
# Usage:
#   chmod +x scripts/install_market_data_deps_manjaro.sh
#   ./scripts/install_market_data_deps_manjaro.sh [--venv]

set -euo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
VENV_DIR="${PROJECT_ROOT}/.venv"
USE_VENV=false

print_help() {
  cat <<'EOF'
ChronoX Market Data Dependencies Installer (Manjaro / Arch)

Options:
  --venv       Create and use a Python virtualenv at .venv (recommended)
  --no-prompt  Don't prompt for confirmation (non-interactive)
  -h|--help    Show this help

Example:
  chmod +x scripts/install_market_data_deps_manjaro.sh
  ./scripts/install_market_data_deps_manjaro.sh --venv

EOF
}

# Parse args
while [[ ${#} -gt 0 ]]; do
  case "$1" in
    --venv)
      USE_VENV=true
      shift
      ;;
    --no-prompt)
      NO_PROMPT=true
      shift
      ;;
    -h|--help)
      print_help
      exit 0
      ;;
    *)
      echo "Unknown arg: $1"
      print_help
      exit 1
      ;;
  esac
done

echo "== ChronoX Market Data Dependencies Installer (Manjaro) =="
echo "Project root: ${PROJECT_ROOT}"

# Ask for sudo early for system package installs
if ! sudo -v; then
  echo "Warning: sudo credentials required for system package installation"
fi

# Update pacman database and install system packages commonly needed for Python builds
echo "\n-> Updating package database and installing system deps via pacman"
sudo pacman -Syu --needed --noconfirm python python-pip python-virtualenv git base-devel pkgconf openssl libffi

# Optional: install nodejs (lightweight-charts backend may require it)
read -r -p "Install optional Node.js (for lightweight-charts) and yarn? [y/N]: " yn || true
yn=${yn:-N}
if [[ "$yn" =~ ^[Yy]$ ]]; then
  echo "Installing nodejs and yarn..."
  sudo pacman -S --needed --noconfirm nodejs npm
  # Yarn alternative: use npm to install yarn globally or pacman package
  sudo npm i -g yarn || true
fi

# Create and activate virtualenv if requested
if [ "${USE_VENV}" = true ]; then
  echo "\n-> Creating virtualenv at ${VENV_DIR}"
  python -m venv "${VENV_DIR}"
  # Activate in current shell for script operations
  # shellcheck disable=SC1090
  source "${VENV_DIR}/bin/activate"
  echo "Activated virtualenv: ${VENV_DIR}"
fi

# Ensure pip is up-to-date
echo "\n-> Ensuring pip is up-to-date"
python -m pip install --upgrade pip setuptools wheel

# Python packages to install
PYTHON_PACKAGES=(
  pandas
  numpy
  mplfinance
  websockets
  aiohttp
  python-dotenv
  finplot
  # tradingpatterns is pulled from PatternPy repo
  "git+https://github.com/keithorange/PatternPy.git"
)

echo "\n-> Installing Python packages via pip"
echo "Packages: ${PYTHON_PACKAGES[*]}"

# Run pip install
python -m pip install "${PYTHON_PACKAGES[@]}"

# Quick check for critical package python-dotenv
python - <<'PY'
try:
    import dotenv
    print('\n✅ python-dotenv is available')
except Exception as e:
    print('\n❌ python-dotenv is NOT installed: ' + str(e))
    raise SystemExit(1)
PY

cat <<'EOF'

=== Installation complete ===

If you used --venv: to use the virtualenv in shells, run:

  source .venv/bin/activate

Then run the visualizer:

  python scripts/backfill_visualizer.py

If the Market Data tab still errors about missing modules, re-run this script or install the missing package manually, e.g.:

  python -m pip install python-dotenv

EOF

exit 0
