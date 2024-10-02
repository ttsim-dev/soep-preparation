import numpy as np
import pandas as pd

from soep_cleaning.initial_preprocessing.helper import (
    _remove_missing_data_categories,
    bool_categorical,
    int_to_int_categorical,
    str_categorical,
)


def test_remove_missing_data_categories_assert_dtype():
    expected = pd.Series(
        ["[0] Cat 0", "[10] Cat 10"],
        dtype="category",
    ).dtype
    sr = pd.Series(
        ["[0] Cat 0", "[10] Cat 10", np.nan],
        dtype="category",
    )
    actual = _remove_missing_data_categories(sr).dtype
    assert actual == expected


def test_remove_missing_data_categories_assert_values():
    expected = pd.Series(
        [pd.NA, pd.NA, "[0] Cat 0", "[10] Cat 10"],
        dtype="category",
    )
    sr = pd.Series(
        ["[-9] Missing 1", "[-1] Missing 2", "[0] Cat 0", "[10] Cat 10"],
        dtype="category",
    )
    actual = _remove_missing_data_categories(sr)
    pd.testing.assert_series_equal(actual, expected)


def test_bool_categorical_assert_dtype():
    expected = pd.Series(
        [True, False],
        dtype="category",
    ).dtype
    sr = pd.Series(
        ["Yes", "No"],
        dtype="category",
    )
    actual = bool_categorical(sr, {"Yes": True, "No": False}).dtype
    assert actual == expected


def test_bool_categorical_assert_renaming():
    expected = pd.Series(
        [True, False],
        dtype="category",
    )
    sr = pd.Series(
        ["Yes", "No"],
        dtype="category",
    )
    actual = bool_categorical(sr, {"Yes": True, "No": False})
    pd.testing.assert_series_equal(actual, expected)


def test_bool_categorical_assert_remove_missing():
    expected = pd.Series(
        [True, False],
        dtype="category",
    ).cat.categories
    sr = pd.Series(
        ["Yes", "No", "[-1] Missing"],
        dtype="category",
    )
    actual = bool_categorical(sr, {"Yes": True, "No": False}).cat.categories
    assert (actual == expected).all()


def test_str_categorical_assert_dtype():
    expected = pd.Series(
        ["Cat 0", "Cat 10"],
        dtype="category",
    ).dtype
    sr = pd.Series(
        ["[0] Cat 0", "[10] Cat 10"],
        dtype="category",
    )
    actual = str_categorical(sr, nr_identifiers=1).dtype
    assert actual == expected


def test_str_categorical_assert_renaming_1_identifier():
    expected = pd.Series(
        ["Cat 0", "Cat 10"],
        dtype="category",
    )
    sr = pd.Series(
        ["[0] Cat 0", "[10] Cat 10"],
        dtype="category",
    )
    actual = str_categorical(sr, nr_identifiers=1)
    pd.testing.assert_series_equal(actual, expected)


def test_str_categorical_assert_renaming_2_identifier():
    expected = pd.Series(
        ["Cat 0", "Cat 10"],
        dtype="category",
    )
    sr = pd.Series(
        ["[0] A Cat 0", "[10] B Cat 10"],
        dtype="category",
    )
    actual = str_categorical(sr, nr_identifiers=2)
    pd.testing.assert_series_equal(actual, expected)


def test_int__to_int_categorical_assert_dtype():
    expected = pd.Series(
        [0, 10],
        dtype="category",
    ).dtype
    sr = pd.Series(
        [0, 10],
    )
    actual = int_to_int_categorical(sr).dtype
    assert actual == expected


def test_int_to_int_categorical_assert_ordering():
    expected = pd.Series([0, 10], dtype="category").cat.as_ordered()
    sr = pd.Series(
        [0, 10],
    )
    actual = int_to_int_categorical(sr, ordered=True)
    pd.testing.assert_series_equal(actual, expected)
