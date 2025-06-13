import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    create_dummy,
)


def test_create_dummy_assert_dtype():
    expected = pd.Series([False, True], dtype="bool[pyarrow]").dtype
    sr = pd.Series(["false", "true"], dtype=object)
    actual = create_dummy(sr, "true").dtype
    assert actual == expected


def test_create_dummy_assert_equal():
    expected = pd.Series([False, True], dtype="bool[pyarrow]")
    sr = pd.Series(["false", "true"], dtype=object)
    actual = create_dummy(sr, "true", "equal")
    pd.testing.assert_series_equal(actual, expected)


def test_create_dummy_assert_neq():
    expected = pd.Series([True, False], dtype="bool[pyarrow]")
    sr = pd.Series(["false", "true"], dtype=object)
    actual = create_dummy(sr, "true", "neq")
    pd.testing.assert_series_equal(actual, expected)


def test_create_dummy_assert_isin():
    expected = pd.Series([False, True, False, True], dtype="bool[pyarrow]")
    sr = pd.Series(["false", "true", "false", "correct"], dtype=object)
    actual = create_dummy(sr, ["true", "correct"], "isin")
    pd.testing.assert_series_equal(actual, expected)


def test_create_dummy_assert_correct_bool():
    expected = pd.Series([False, True], dtype="bool[pyarrow]")
    sr = pd.Series([False, True], dtype=object)
    actual = create_dummy(sr, true_value=True, kind="equal")
    pd.testing.assert_series_equal(actual, expected)
