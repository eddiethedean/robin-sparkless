.PHONY: build test test-rust test-python sparkless-parity bench-python clean check fmt clippy audit outdated deny all

# Use stable toolchain when no default is configured (override with RUSTUP_TOOLCHAIN=nightly etc.)
export RUSTUP_TOOLCHAIN ?= stable

# Build
build:
	cargo build

build-release:
	cargo build --release

# Run Rust tests only
test-rust:
	cargo test

# Run Python tests (creates .venv, installs extension with maturin, runs pytest)
test-python:
	@if [ ! -d .venv ]; then python3 -m venv .venv; fi
	. .venv/bin/activate && pip install -q maturin pytest && maturin develop --features pyo3
	. .venv/bin/activate && pytest tests/python/ -v

# Run all tests (Rust + Python)
test: test-rust test-python

# Compare robin-sparkless vs sparkless performance. Use .venv-sparkless for both backends (pip install sparkless + maturin develop).
# Quick run (smaller sizes): . .venv-sparkless/bin/activate && python scripts/bench_robin_vs_sparkless.py --quick
bench-python:
	@if [ ! -d .venv-sparkless ]; then python3 -m venv .venv-sparkless; fi
	. .venv-sparkless/bin/activate && pip install -q maturin sparkless && maturin develop --features pyo3
	. .venv-sparkless/bin/activate && python scripts/bench_robin_vs_sparkless.py

# Run parity tests (optionally convert from Sparkless expected_outputs first).
# Set SPARKLESS_EXPECTED_OUTPUTS=/path/to/sparkless/tests/expected_outputs to run converter.
sparkless-parity:
	@if [ -n "$$SPARKLESS_EXPECTED_OUTPUTS" ]; then \
		mkdir -p tests/fixtures/converted; \
		python3 tests/convert_sparkless_fixtures.py --batch "$$SPARKLESS_EXPECTED_OUTPUTS" tests/fixtures --output-subdir converted; \
	fi
	cargo test pyspark_parity_fixtures

# Run all checks (lint, format, security, deny, tests)
check: fmt clippy audit deny test
	@echo "All checks passed"

# Format code
fmt:
	cargo fmt
	@echo "Formatted"

# Check format without modifying
fmt-check:
	cargo fmt --check

# Lint with Clippy
clippy:
	cargo clippy -- -D warnings

# Security: scan for known vulnerabilities
audit:
	cargo audit

# List outdated dependencies
outdated:
	cargo outdated

# Advisory, bans, sources checks (skip licenses - needs per-crate clarifications)
deny:
	cargo deny check advisories bans sources

# Run everything: format, lint, security, deny, tests
all: check
	@echo "All updates and checks complete"

# Clean
clean:
	cargo clean
	rm -rf target/
