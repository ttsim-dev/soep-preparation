import numpy as np
import pandas as pd
import pyarrow as pa

from soep_preparation.utilities.series_manipulator import (
    _remove_missing_data_values,
)


def test_remove_missing_data_values_assert_dtype():
    expected_categories = pd.array(
        ["[0] Cat 0", "[10] Cat 10"],
        dtype=pd.ArrowDtype(pa.string()),
    )
    expected = pd.Series(expected_categories).dtype
    sr = pd.Series(
        ["[0] Cat 0", "[10] Cat 10", np.nan],
        dtype=pd.ArrowDtype(pa.string()),
    )
    actual = _remove_missing_data_values(sr).dtype
    assert actual == expected


def test_remove_missing_data_values_assert_values():
    expected_categories = pd.array(
        [pd.NA, pd.NA, "[0] Cat 0", "[10] Cat 10"],
        dtype=pd.ArrowDtype(pa.string()),
    )
    expected = pd.Series(expected_categories)
    sr = pd.Series(
        ["[-9] Missing 1", "[-1] Missing 2", "[0] Cat 0", "[10] Cat 10"],
        dtype=pd.ArrowDtype(pa.string()),
    )
    actual = _remove_missing_data_values(sr)
    pd.testing.assert_series_equal(actual, expected)
