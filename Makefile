.PHONY: build test clean check fmt clippy audit outdated deny all

# Set RUSTUP_TOOLCHAIN=stable if not configured. Run: RUSTUP_TOOLCHAIN=stable make check

# Build
build:
	cargo build

build-release:
	cargo build --release

# Run all tests
test:
	cargo test

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
