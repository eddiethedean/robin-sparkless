# sparkless-server-bin compatibility accessors

This universal, lightweight distribution preserves `sparkless_server_bin.executable_path()`
for existing applications. It forwards to the separately installed base `sparkless-server`
package and carries no native executable. Its `quack_extension_path()` helper forwards only
when `sparkless-server-duckdb` is installed; otherwise it raises an install hint. Use the
product package directly for new applications.
