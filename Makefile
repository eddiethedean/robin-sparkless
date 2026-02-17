.PHONY: build build-release test test-rust check check-full fmt fmt-check clippy audit outdated deny \
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

# Run Rust tests only
test-rust:
	cargo test

# Run all tests (Rust only)
test: test-rust

# Run all Rust checks (format check, clippy, audit, deny, Rust tests).
check: fmt-check clippy audit deny test-rust
	@echo "All checks passed"

# Backwards-compatible alias for full check suite (historically included Python).
# Now runs the Rust-only checks defined in `check`.
check-full: check
	@echo "check-full (Rust-only): format, clippy, audit, deny, tests"

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
	cargo test pyspark_parity_fixtures

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

