import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    object_to_float,
    object_to_int,
)


def test_object_to_float_assert_dtype():
    expected = pd.Series([0.1, 0.2], dtype="double[pyarrow]").dtype
    sr = pd.Series([0.1, 0.2], dtype=object)
    actual = object_to_float(sr).dtype
    assert actual == expected


def test_object_to_float_assert_remove_missing_str():
    expected = pd.Series([0.1, 0.2, pd.NA], dtype="double[pyarrow]")
    sr = pd.Series([0.1, 0.2, "[-1] Missing"], dtype=object)
    actual = object_to_float(sr)
    pd.testing.assert_series_equal(actual, expected)


def test_object_to_float_assert_remove_missing_int():
    expected = pd.Series([0.1, 0.2, pd.NA], dtype="double[pyarrow]")
    sr = pd.Series([0.1, 0.2, -1], dtype=object)
    actual = object_to_float(sr)
    pd.testing.assert_series_equal(actual, expected)


def test_object_to_float_assert_remove_missing_float():
    expected = pd.Series([0.1, 0.2, pd.NA], dtype="double[pyarrow]")
    sr = pd.Series([0.1, 0.2, -0.1], dtype=object)
    actual = object_to_float(sr)
    pd.testing.assert_series_equal(actual, expected)


def test_object_to_int_assert_dtype():
    expected = pd.Series([1, 2], dtype="int8[pyarrow]").dtype
    sr = pd.Series([1, 2], dtype=object)
    actual = object_to_int(sr).dtype
    assert actual == expected


def test_object_to_int_assert_remove_missing_str():
    expected = pd.Series([1, 2, pd.NA], dtype="int8[pyarrow]")
    sr = pd.Series([1, 2, "[-1] Missing"], dtype=object)
    actual = object_to_int(sr)
    pd.testing.assert_series_equal(actual, expected)


def test_object_to_int_assert_remove_missing_int():
    expected = pd.Series([1, 2, pd.NA], dtype="int8[pyarrow]")
    sr = pd.Series([1, 2, -1], dtype=object)
    actual = object_to_int(sr)
    pd.testing.assert_series_equal(actual, expected)


def test_object_to_int_assert_remove_missing_float():
    expected = pd.Series([1, 2, pd.NA], dtype="int8[pyarrow]")
    sr = pd.Series([1, 2, -0.1], dtype=object)
    actual = object_to_int(sr)
    pd.testing.assert_series_equal(actual, expected)


def test_object_to_int_with_renaming_remaps_labels_to_codes():
    expected = pd.Series([2, 1, 0], dtype="int8[pyarrow]")
    sr = pd.Series(["[1] Stark", "[2] Ein wenig", "[3] Gar nicht"], dtype=object)
    actual = object_to_int(
        sr,
        renaming={"[1] Stark": 2, "[2] Ein wenig": 1, "[3] Gar nicht": 0},
    )
    pd.testing.assert_series_equal(actual, expected)


def test_object_to_int_with_renaming_keeps_missing_as_arrow_na():
    expected = pd.Series([1, pd.NA], dtype="int8[pyarrow]")
    sr = pd.Series(["[1] January", "[-1] Missing"], dtype=object)
    actual = object_to_int(sr, renaming={"[1] January": 1})
    pd.testing.assert_series_equal(actual, expected)
