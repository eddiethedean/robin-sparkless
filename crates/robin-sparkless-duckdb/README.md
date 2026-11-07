# robin-sparkless-duckdb

Optional, read-only DuckDB catalog source for Robin Sparkless. It maps trusted
standalone database files or a configured shared Quack owner to the engine's
Arrow IPC catalog contract. It is independent of the Polars execution crate and
does not provide DuckDB query execution or writes.

Use the [DuckDB source guide](https://robin-sparkless.readthedocs.io/en/latest/DUCKDB_SOURCE/)
for server configuration and the [execution guide](https://robin-sparkless.readthedocs.io/en/latest/DUCKDB_EXECUTION/)
for the 4.20 server product’s optional DuckDB backend.
