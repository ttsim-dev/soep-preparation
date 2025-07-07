import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    apply_smallest_float_dtype,
    apply_smallest_int_dtype,
    float_to_int,
)


def test_apply_smallest_float_dtype_assert_dtype():
    expected = pd.Series([0.1, 0.2], dtype="float[pyarrow]").dtype
    sr = pd.Series([0.1, 0.2])
    actual = apply_smallest_float_dtype(sr).dtype
    assert actual == expected


def test_apply_smallest_int_dtype_assert_dtype_int8():
    expected = pd.Series([-128, 127], dtype="int8[pyarrow]").dtype
    sr = pd.Series([-128, 127])
    actual = apply_smallest_int_dtype(sr).dtype
    assert actual == expected


def test_apply_smallest_int_dtype_assert_dtype_int16():
    expected = pd.Series([-32768, 32767], dtype="int16[pyarrow]").dtype
    sr = pd.Series([-32768, 32767])
    actual = apply_smallest_int_dtype(sr).dtype
    assert actual == expected


def test_apply_smallest_int_dtype_assert_dtype_uint8():
    expected = pd.Series([0, 255], dtype="uint8[pyarrow]").dtype
    sr = pd.Series([0, 255])
    actual = apply_smallest_int_dtype(sr).dtype
    assert actual == expected


def test_apply_smallest_int_dtype_assert_dtype_uint16():
    expected = pd.Series([0, 65535], dtype="uint16[pyarrow]").dtype
    sr = pd.Series([0, 65535])
    actual = apply_smallest_int_dtype(sr).dtype
    assert actual == expected


def test_float_to_int_assert_dtype():
    expected = pd.Series([0, 1], dtype="uint8[pyarrow]").dtype
    sr = pd.Series([0.0, 1.0])
    actual = float_to_int(sr).dtype
    assert actual == expected


def test_float_to_int_assert_remove_missing_values():
    expected = pd.Series([0, 1, pd.NA], dtype="uint8[pyarrow]")
    sr = pd.Series([0.0, 1.0, -1], dtype="float[pyarrow]")
    actual = float_to_int(sr, drop_missing=True)
    pd.testing.assert_series_equal(actual, expected)
