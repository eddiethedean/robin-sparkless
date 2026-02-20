.PHONY: build build-release build-all-features test test-rust check check-full fmt fmt-check clippy audit outdated deny \
	test-parity-phase-a test-parity-phase-b test-parity-phase-c test-parity-phase-d \
	test-parity-phase-e test-parity-phase-f test-parity-phase-g test-parity-phases \
	sparkless-parity all

# Use stable toolchain when no default is configured (override with RUSTUP_TOOLCHAIN=nightly etc.)
export RUSTUP_TOOLCHAIN ?= stable
# Use real Cargo home to avoid sandbox cache corruption (e.g. Cursor IDE).
# Force override (:=) so Cursor/sandbox CARGO_HOME does not take precedence.
export CARGO_HOME := $(HOME)/.cargo

# Build
build:
	cargo build

build-release:
	cargo build --release

# Build with all optional features (sql, delta) so feature-gated code is compiled and checked
build-all-features:
	cargo build --workspace --all-features

# Run Rust tests only (default features)
test-rust:
	cargo test --workspace

# Run Rust tests with all features (sql, delta) - used by check
test-rust-all-features:
	cargo test --workspace --all-features

# Run all tests (Rust only)
test: test-rust

# Run all Rust checks. Fast steps first (fmt, audit, deny), then one compile: clippy --all-targets
# builds lib + tests with all features; cargo test reuses that and only runs tests.
# Cargo is incremental: only crates with changed sources (or dependents) recompile. Avoid
# "cargo clean" so repeated "make check" reuses the previous build where possible.
check: fmt-check audit deny clippy test-rust-all-features
	@echo "All checks passed"

# Backwards-compatible alias for full check suite (historically included Python).
# Now runs the Rust-only checks defined in `check`.
check-full: check
	@echo "check-full (Rust-only): format, clippy, audit, deny, tests"

# Format code
fmt:
	cargo fmt
	@echo "Formatted"

# Check format without modifying (workspace is formatted as a whole from root)
fmt-check:
	cargo fmt --check

# Lint with Clippy (all features, all targets including tests so one compile is reused by test)
clippy:
	cargo clippy --workspace --all-features --all-targets -- -D warnings

# Security: scan for known vulnerabilities
audit:
	cargo audit

# List outdated dependencies
outdated:
	cargo outdated

# Advisory, bans, sources checks (skip licenses - needs per-crate clarifications). Workspace-aware.
deny:
	cargo deny check advisories bans sources

# Run parity fixtures for a specific phase (A–G). See docs/PARITY_STATUS.md.
test-parity-phase-a:
	PARITY_PHASE=a cargo test pyspark_parity_fixtures

test-parity-phase-b:
	PARITY_PHASE=b cargo test pyspark_parity_fixtures

test-parity-phase-c:
	PARITY_PHASE=c cargo test pyspark_parity_fixtures

test-parity-phase-d:
	PARITY_PHASE=d cargo test pyspark_parity_fixtures

test-parity-phase-e:
	PARITY_PHASE=e cargo test pyspark_parity_fixtures

test-parity-phase-f:
	PARITY_PHASE=f cargo test pyspark_parity_fixtures

test-parity-phase-g:
	PARITY_PHASE=g cargo test pyspark_parity_fixtures

test-parity-phases:
	cargo test --workspace --all-features pyspark_parity_fixtures

# Convert Sparkless fixtures (when SPARKLESS_EXPECTED_OUTPUTS is set), regenerate
# expected results from PySpark, then run Rust parity tests.
sparkless-parity:
	@if [ -z "$$SPARKLESS_EXPECTED_OUTPUTS" ]; then \
	  echo "SPARKLESS_EXPECTED_OUTPUTS is not set; see docs/CONVERTER_STATUS.md"; \
	  exit 1; \
	fi
	python tests/convert_sparkless_fixtures.py --batch "$$SPARKLESS_EXPECTED_OUTPUTS" tests/fixtures --output-subdir converted --dedupe
	python tests/regenerate_expected_from_pyspark.py tests/fixtures/converted
	cargo test pyspark_parity_fixtures

# Run everything: format, lint, security, deny, tests
all: check
	@echo "All updates and checks complete"

