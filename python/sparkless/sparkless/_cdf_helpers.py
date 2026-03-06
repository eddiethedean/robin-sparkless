# Helpers for createDataFrame from Rust: run dict.get(k) in Python so key lookup is correct.


def dict_rows_to_column_order(data, keys):
    """Convert list of dicts to list of lists in keys order. Called from Rust (#1267)."""
    return [[d.get(k) for k in keys] for d in data]
