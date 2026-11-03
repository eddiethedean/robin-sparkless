#!/usr/bin/env bash
set -euo pipefail

# Strict type checking for the Python sparkless library.
# Runs mypy against the local API, testing package, and shared launcher.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${ROOT_DIR}"

exec mypy \
  --config-file python/pyproject.toml \
  --check-untyped-defs \
  python/sparkless/sparkless \
  python/testing/sparkless_testing \
  python/connect/sparkless_connect

