## Typing Guide for `sparkless`

This project targets **Python 3.8+** and maintains type hints for both:

- The public Python package `sparkless` (PySpark-like API), and
- The Rust-backed native module exposed via `sparkless._native`.

Type checking is performed with **mypy** and **pyright**.

### Targets and guarantees

- **Library code** (`python/sparkless/sparkless`):
  - Public functions, classes, and modules are expected to have **precise types**.
  - mypy runs in a stricter mode with `--check-untyped-defs` via
    `scripts/typecheck_strict.sh`, which checks the `sparkless` package only.
- **Tests and helper scripts** (`tests/`, `scripts/`):
  - Typing is **best-effort**; annotations are added where they materially improve
    safety or readability, but some `Any` is tolerated.

### Running type checkers

- **Strict mypy for library code**:
  - `scripts/typecheck_strict.sh`
  - Uses `python/pyproject.toml` as the mypy config and runs with
    `--check-untyped-defs` on `python/sparkless/sparkless`.
- **pyright**:
  - Configured via `[tool.pyright]` in `python/pyproject.toml` with
    `pythonVersion = "3.8"` and `typeCheckingMode = "basic"`.
  - Tests and `upstream_sparkless` are excluded.

### Python version and syntax

- Code is written to be compatible with **Python 3.8+**.
- In modules that use modern type syntax such as `list[str]` or
  `dict[str, Any]`, ensure `from __future__ import annotations` is present.
- In older or simple modules, `List[...]` and `Dict[...]` from `typing`
  are also acceptable; avoid mixing styles within the same file.

### Imports and circular dependencies

- Use `if TYPE_CHECKING:` for **heavy or circular imports**, especially:
  - `SparkSession`, `DataFrame`, `Column`, `WindowSpec`, and schema types.
  - Native bindings in `sparkless._native` when only needed for hints.
- At runtime, prefer importing:
  - `sparkless._native` for low-level functions, and
  - High‑level APIs from `sparkless` or `sparkless.sql.*` for user-facing code.

### Common aliases and patterns

- Re‑use shared aliases where possible:
  - `ColumnOrName = Union[_ColumnType, str]`
  - Schema types (`StructType`, `StructField`, `DataType`, etc.) from
    `sparkless.sql.types`.
- When returning columns from native calls:
  - Use helper functions that cast `Any` to the concrete column type (e.g.
    `_col_result(...)`) to avoid `no-any-return` violations.

### Exceptions and config

- Exception aliases live in `sparkless.errors`:
  - `AnalysisException`, `PySparkValueError`, `PySparkTypeError`,
    `PySparkRuntimeError`, `IllegalArgumentException`.
  - These are typed as `Type[BaseException]` and all alias the native
    `SparklessError` (or a `RuntimeError` fallback).
- Configuration helpers in `sparkless.config`:
  - Use `FeatureFlagValue = Union[bool, str, int]` and
    `FeatureFlagOverrides = Dict[str, FeatureFlagValue]`.
  - Public helpers such as `get_feature_flag_overrides()` return a
    read‑only `Mapping[str, FeatureFlagValue]`.

### Contributor guidelines

- When adding or changing public APIs:
  - Prefer explicit parameter and return types over `Any`.
  - Use shared aliases (e.g. `ColumnOrName`) rather than repeating unions.
  - Keep annotations 3.8‑compatible (`from __future__ import annotations`
    plus modern syntax, or `typing.List` / `typing.Dict`).
- For new modules:
  - Add `from __future__ import annotations` at the top.
  - Consider adding a short module‑level docstring describing the main types.
- For stubs or native-backed functions:
  - Use `cast(...)` where necessary to satisfy mypy without changing runtime
    behavior.

For more details, see `python/pyproject.toml` (mypy/pyright settings) and
`scripts/typecheck_strict.sh` for the strict mypy entry point.

