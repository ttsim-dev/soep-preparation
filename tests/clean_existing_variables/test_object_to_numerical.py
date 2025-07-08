import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    object_to_float,
    object_to_int,
)


def test_object_to_float_assert_dtype():
    expected = pd.Series([0.1, 0.2], dtype="float[pyarrow]").dtype
    sr = pd.Series([0.1, 0.2], dtype=object)
    actual = object_to_float(sr).dtype
    assert actual == expected


def test_object_to_float_assert_remove_missing_str():
    expected = pd.Series([0.1, 0.2, pd.NA], dtype="float[pyarrow]")
    sr = pd.Series([0.1, 0.2, "[-1] Missing"], dtype=object)
    actual = object_to_float(sr)
    pd.testing.assert_series_equal(actual, expected)


def test_object_to_float_assert_remove_missing_int():
    expected = pd.Series([0.1, 0.2, pd.NA], dtype="float[pyarrow]")
    sr = pd.Series([0.1, 0.2, -1], dtype=object)
    actual = object_to_float(sr)
    pd.testing.assert_series_equal(actual, expected)


def test_object_to_float_assert_remove_missing_float():
    expected = pd.Series([0.1, 0.2, pd.NA], dtype="float[pyarrow]")
    sr = pd.Series([0.1, 0.2, -0.1], dtype=object)
    actual = object_to_float(sr)
    pd.testing.assert_series_equal(actual, expected)


def test_object_to_int_assert_dtype():
    expected = pd.Series([1, 2], dtype="uint8[pyarrow]").dtype
    sr = pd.Series([1, 2], dtype=object)
    actual = object_to_int(sr).dtype
    assert actual == expected


def test_object_to_int_assert_remove_missing_str():
    expected = pd.Series([1, 2, pd.NA], dtype="uint8[pyarrow]")
    sr = pd.Series([1, 2, "[-1] Missing"], dtype=object)
    actual = object_to_int(sr)
    pd.testing.assert_series_equal(actual, expected)


def test_object_to_int_assert_remove_missing_int():
    expected = pd.Series([1, 2, pd.NA], dtype="uint8[pyarrow]")
    sr = pd.Series([1, 2, -1], dtype=object)
    actual = object_to_int(sr)
    pd.testing.assert_series_equal(actual, expected)


def test_object_to_int_assert_remove_missing_float():
    expected = pd.Series([1, 2, pd.NA], dtype="uint8[pyarrow]")
    sr = pd.Series([1, 2, -0.1], dtype=object)
    actual = object_to_int(sr)
    pd.testing.assert_series_equal(actual, expected)
