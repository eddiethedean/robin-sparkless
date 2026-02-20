.PHONY: build build-release build-all-features test test-rust check check-full check-crate fmt fmt-check clippy audit outdated deny \
	clean \
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

# Run all Rust checks. Clean first so old binaries don't accumulate (each run is a full rebuild).
# Then run fmt-check, audit, deny, and clippy in parallel (they are independent); then tests
# (tests reuse clippy's build). Use -j4 to overlap the four jobs.
check: clean
	@$(MAKE) -j4 fmt-check audit deny clippy
	$(MAKE) test-rust-all-features
	@echo "All checks passed"

# Remove all build artifacts (target/). Use when target/ grows large from repeated
# check-full runs (incremental + debug + all-features + all-targets). Next build will be a full rebuild.
clean:
	cargo clean

# Run checks for a single crate only (faster when editing one crate).
# Usage: make check-crate CRATE=spark-sql-parser
# Runs: fmt-check, audit, deny, then clippy and test for that package only.
check-crate:
	@if [ -z "$(CRATE)" ]; then echo "Usage: make check-crate CRATE=<package-name>"; exit 1; fi
	$(MAKE) fmt-check
	$(MAKE) audit
	$(MAKE) deny
	cargo clippy -p $(CRATE) --all-features --all-targets -- -D warnings
	cargo test -p $(CRATE) --all-features
	@echo "check-crate ($(CRATE)): format, clippy, audit, deny, tests passed"

# Backwards-compatible alias for full check suite (historically included Python).
# With CRATE set, runs checks for that package only (faster when editing one crate).
# Usage: make check-full  OR  make check-full CRATE=spark-sql-parser
check-full:
	@if [ -n "$(CRATE)" ]; then \
	  $(MAKE) check-crate CRATE=$(CRATE); \
	  echo "check-full (crate $(CRATE)): format, clippy, audit, deny, tests"; \
	else \
	  $(MAKE) check; \
	  echo "check-full (Rust-only): format, clippy, audit, deny, tests"; \
	fi

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

