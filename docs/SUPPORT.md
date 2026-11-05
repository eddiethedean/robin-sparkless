# Support and compatibility policy

Sparkless is maintained as an open-source project on a best-effort basis.
There is no published SLA, paid support commitment, or guaranteed security-fix
backport schedule. Evaluate your required response and lifecycle guarantees
before enterprise adoption. The current release is 4.19.0. See the [versioned support matrix](SUPPORT_MATRIX.md).

Use [GitHub Issues](https://github.com/eddiethedean/robin-sparkless/issues) for bugs
and feature requests. Include package/interpreter/OS/architecture versions,
actual backend identity, minimal input/schema/code, expected Spark behavior,
actual output/exception and relevant build features. Remove secrets and private data.
For install failures include wheel filename/native module path and complete error.

Maintainers prioritize reproducible failures against current releases and the
reviewed development branch. Older releases may require upgrading before a fix
can be assessed. No long-term support branch is promised. Minor releases can change package, Python, or Rust interfaces. Pin a version you
have tested and review migration notes before expanding your version range.

Deprecations should be recorded in release notes with a replacement and removal
version before removal when practical. Security fixes or correctness defects can
require immediate behavior changes; release notes must explain migration impact.
Experimental or explicitly unsupported APIs do not carry a full Spark compatibility
promise. Compare your workload against your production Spark oracle on upgrades.

See [security reporting](SECURITY.md), [release notes](https://github.com/eddiethedean/robin-sparkless/blob/main/CHANGELOG.md),
and [contribution workflow](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md).
