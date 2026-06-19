import pandas as pd
import pyarrow as pa

from soep_preparation.utilities.data_manipulator import (
    replace_missing_codes_with_na,
)


def test_replace_missing_codes_with_na_assert_dtype():
    expected_categories = pd.array(
        ["[0] Cat 0", "[10] Cat 10"],
        dtype=pd.ArrowDtype(pa.string()),
    )
    expected = pd.Series(expected_categories).dtype
    sr = pd.Series(
        ["[0] Cat 0", "[10] Cat 10", pd.NA],
        dtype=pd.ArrowDtype(pa.string()),
    )
    actual = replace_missing_codes_with_na(sr).dtype
    assert actual == expected


def test_replace_missing_codes_with_na_assert_values():
    expected_categories = pd.array(
        [pd.NA, pd.NA, "[0] Cat 0", "[10] Cat 10"],
        dtype=pd.ArrowDtype(pa.string()),
    )
    expected = pd.Series(expected_categories)
    sr = pd.Series(
        ["[-9] Missing 1", "[-1] Missing 2", "[0] Cat 0", "[10] Cat 10"],
        dtype=pd.ArrowDtype(pa.string()),
    )
    actual = replace_missing_codes_with_na(sr)
    pd.testing.assert_series_equal(actual, expected)
