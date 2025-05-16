import numpy as np
import pandas as pd
import pyarrow as pa

from soep_preparation.utilities import (
    _remove_missing_data_values,
    object_to_bool_categorical,
    object_to_int_categorical,
    object_to_str_categorical,
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


def test_object_to_bool_categorical_assert_dtype():
    expected_categories = pd.array([True, False], dtype=pd.ArrowDtype(pa.bool_()))
    expected = pd.Series(pd.Categorical(expected_categories)).dtype
    sr = pd.Series(["Yes", "No"], dtype=object)
    actual = object_to_bool_categorical(sr, {"Yes": True, "No": False}).dtype
    assert actual == expected


def test_object_to_bool_categorical_assert_renaming():
    expected_categories = pd.array([True, False], dtype=pd.ArrowDtype(pa.bool_()))
    expected = pd.Series(pd.Categorical(expected_categories))
    sr = pd.Series(["Yes", "No"], dtype=object)
    actual = object_to_bool_categorical(sr, {"No": False, "Yes": True})
    pd.testing.assert_series_equal(actual, expected)


def test_object_to_bool_categorical_assert_remove_missing():
    expected_categories = pd.array([True, False], dtype=pd.ArrowDtype(pa.bool_()))
    expected = pd.Series(
        pd.Categorical(expected_categories, ordered=True),
    ).cat.categories
    sr = pd.Series(["Yes", "No", "[-1] Missing"], dtype=object)
    actual = object_to_bool_categorical(sr, {"No": False, "Yes": True}).cat.categories
    assert (actual == expected).all()


def test_object_to_str_categorical_assert_dtype():
    expected_categories = pd.array(
        ["Cat 0", "Cat 10"],
        dtype=pd.ArrowDtype(pa.string()),
    )
    expected = pd.Series(pd.Categorical(expected_categories, ordered=True)).dtype
    sr = pd.Series(["[0] Cat 0", "[10] Cat 10"], dtype=object)
    actual = object_to_str_categorical(sr, nr_identifiers=1, ordered=True).dtype
    assert actual == expected


def test_object_to_str_categorical_assert_renaming_1_identifier():
    expected_categories = pd.array(
        ["Cat 0", "Cat 10"],
        dtype=pd.ArrowDtype(pa.string()),
    )
    expected = pd.Series(pd.Categorical(expected_categories, ordered=True))
    sr = pd.Series(["[0] Cat 0", "[10] Cat 10"], dtype=object)
    actual = object_to_str_categorical(sr, nr_identifiers=1, ordered=True)
    pd.testing.assert_series_equal(actual, expected)


def test_object_to_str_categorical_assert_renaming_2_identifier():
    expected_categories = pd.array(
        ["Cat 0", "Cat 10"],
        dtype=pd.ArrowDtype(pa.string()),
    )
    expected = pd.Series(pd.Categorical(expected_categories, ordered=True))
    sr = pd.Series(["[0] A Cat 0", "[10] B Cat 10"], dtype=object)
    actual = object_to_str_categorical(sr, nr_identifiers=2, ordered=True)
    pd.testing.assert_series_equal(actual, expected)


def test_object_to_int_categorical_assert_dtype():
    expected_categories = pd.array([0, 10], dtype=pd.ArrowDtype(pa.uint8()))
    expected = pd.Series(pd.Categorical(expected_categories)).dtype
    sr = pd.Series([0, 10], dtype=object)
    actual = object_to_int_categorical(sr).dtype
    assert actual == expected


def test_object_to_int_categorical_assert_ordering():
    expected_categories = pd.array([0, 10], dtype=pd.ArrowDtype(pa.uint8()))
    expected = pd.Series(pd.Categorical(expected_categories, ordered=True))
    sr = pd.Series([0, 10], dtype=object)
    actual = object_to_int_categorical(sr, ordered=True)
    pd.testing.assert_series_equal(actual, expected)
