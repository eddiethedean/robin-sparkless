# Sparkless native engine

This package contains Sparkless's platform-specific Rust/Polars extension.
Install [`sparkless`](https://pypi.org/project/sparkless/) for the Python DataFrame
API; it selects the exact compatible native-engine version automatically.

The native wheels use the CPython 3.11 stable ABI and support CPython 3.11–3.14
on Linux glibc/musl, macOS, and Windows, on x86_64 and ARM64. Source builds require
the repository's pinned Rust toolchain. The source archive includes the fixed
compression dependency graph used by the official wheels.

See the [installation guide](https://robin-sparkless.readthedocs.io/en/latest/INSTALLATION/)
and [source repository](https://github.com/eddiethedean/robin-sparkless).
