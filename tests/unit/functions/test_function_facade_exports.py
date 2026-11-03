"""Guard the SQL function facade while implementations live in private modules."""

import pytest

from sparkless_testing import get_imports


def test_facade_keeps_historical_callable_attributes():
    F = get_imports().F
    # These operations are used directly through ``F`` but are not all in
    # ``__all__``. The native column ``over`` binding also looks up its helper
    # on this facade at runtime.
    names = (
        "slice",
        "percentile_approx",
        "variance",
        "instr",
        "sqrt",
        "ceil",
        "window",
    )

    missing = [name for name in names if not callable(getattr(F, name, None))]

    assert not missing


@pytest.mark.sparkless_only
def test_facade_keeps_native_window_helper():
    F = get_imports().F

    assert callable(getattr(F, "_window_spec_to_partition_order", None))
