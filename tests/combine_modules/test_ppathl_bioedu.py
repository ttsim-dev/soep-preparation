import pandas as pd

from soep_preparation.combine_modules.ppathl_bioedu import combine


def _ppathl() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "p_id": pd.Series([1, 2, 3], dtype="int32[pyarrow]"),
            "birth_month_ppathl": pd.Series([3, pd.NA, pd.NA], dtype="int8[pyarrow]"),
        }
    )


def _bioedu() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "p_id": pd.Series([1, 2, 3], dtype="int32[pyarrow]"),
            "birth_month_bioedu": pd.Series([pd.NA, 7, pd.NA], dtype="int8[pyarrow]"),
        }
    )


def test_combine_birth_month_is_plain_integer_dtype():
    """`birth_month` is a plain pyarrow integer, so missings show as `<NA>`."""
    out = combine(ppathl=_ppathl(), bioedu=_bioedu())
    assert out["birth_month"].dtype == pd.Series([1], dtype="int8[pyarrow]").dtype


def test_combine_birth_month_prefers_ppathl_then_fills_from_bioedu():
    """`birth_month` takes ppathl where present, else bioedu, else `<NA>`."""
    out = combine(ppathl=_ppathl(), bioedu=_bioedu())
    expected = pd.Series([3, 7, pd.NA], dtype="int8[pyarrow]", name="birth_month")
    pd.testing.assert_series_equal(out["birth_month"], expected)
