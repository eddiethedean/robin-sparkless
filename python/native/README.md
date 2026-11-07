# Sparkless native engine

This package contains Sparkless's platform-specific Rust/Polars extension.
Install [`sparkless`](https://pypi.org/project/sparkless/) for the Python DataFrame
API. Sparkless 4.20.0 installs `sparkless-native==4.20.0` automatically.

The native wheels use the CPython 3.11 stable ABI and support CPython 3.11–3.14
on Linux glibc/musl, macOS, and Windows, on x86_64 and ARM64. Check the
[wheel files](https://pypi.org/project/sparkless-native/4.20.0/#files) for your platform.
Source builds require the repository's pinned Rust toolchain. The source archive
includes the compression dependency graph used by the official wheels.

See the [installation guide](https://robin-sparkless.readthedocs.io/en/latest/INSTALLATION/)
and [source repository](https://github.com/eddiethedean/robin-sparkless).
