# sparkless-server-bin

This distribution contains the prebuilt `sparkless-server` executable for one
specific OS and architecture. It is installed automatically by
`sparkless[server]`; install it directly only when you want to manage the binary
package separately.

The package is published as `py3-none` platform-specific wheels. Its executable
does not depend on a CPython ABI, so one platform wheel serves Python 3.8 and
newer. The 4.16 release workflow builds Linux x86_64 (`manylinux_2_35`), Windows
x86_64, macOS 15+ arm64, and macOS 15+ x86_64 wheels. Source builds require a
matching executable supplied through `SPARKLESS_SERVER_BINARY`; they never
download or compile the server implicitly.
Install `sparkless` without the `server` extra on platforms without a matching
binary wheel.

The executable is a native Rust HTTP SQL service. It does not require Python or
a JVM at runtime. Its supported SQL subset and resource limits are documented
in `crates/sparkless-server/README.md` in the source repository.
