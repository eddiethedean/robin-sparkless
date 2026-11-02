# robin-sparkless-duckdb

Optional, read-only DuckDB catalog source for Robin Sparkless. It maps trusted
standalone database files or a configured shared Quack owner to the engine's
Arrow IPC catalog contract. It is independent of the Polars execution crate and
does not provide DuckDB query execution or writes.
