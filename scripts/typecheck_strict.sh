#!/usr/bin/env bash
set -euo pipefail

# Strict type checking for the Python sparkless library.
# Runs mypy with --check-untyped-defs against the core package only,
# using the configuration in python/pyproject.toml.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${ROOT_DIR}"

exec mypy \
  --config-file python/pyproject.toml \
  --check-untyped-defs \
  python/sparkless/sparkless

