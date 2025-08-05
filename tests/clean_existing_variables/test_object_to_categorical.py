import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    object_to_bool_categorical,
    object_to_int_categorical,
    object_to_str_categorical,
)


def test_object_to_bool_categorical_assert_dtype():
    expected_categories = pd.array([True, False], dtype="bool[pyarrow]")
    expected = pd.Series(pd.Categorical(expected_categories)).dtype
    sr = pd.Series(["Yes", "No"], dtype=object)
    actual = object_to_bool_categorical(sr, {"Yes": True, "No": False}).dtype
    assert actual == expected


def test_object_to_bool_categorical_assert_renaming():
    expected_categories = pd.array([True, False], dtype="bool[pyarrow]")
    expected = pd.Series(pd.Categorical(expected_categories))
    sr = pd.Series(["Yes", "No"], dtype=object)
    actual = object_to_bool_categorical(sr, {"No": False, "Yes": True})
    pd.testing.assert_series_equal(actual, expected)


def test_object_to_bool_categorical_assert_remove_missing():
    expected_categories = pd.array([True, False], dtype="bool[pyarrow]")
    expected = pd.Series(
        pd.Categorical(expected_categories, ordered=True),
    ).cat.categories
    sr = pd.Series(["Yes", "No", "[-1] Missing"], dtype=object)
    actual = object_to_bool_categorical(sr, {"No": False, "Yes": True}).cat.categories
    assert (actual == expected).all()


def test_object_to_str_categorical_assert_dtype():
    expected_categories = pd.array(
        ["Cat 0", "Cat 10"],
        dtype="string[pyarrow]",
    )
    expected = pd.Series(pd.Categorical(expected_categories, ordered=True)).dtype
    sr = pd.Series(["[0] Cat 0", "[10] Cat 10"], dtype=object)
    actual = object_to_str_categorical(sr, nr_identifiers=1, ordered=True).dtype
    assert actual == expected


def test_object_to_str_categorical_assert_renaming_1_identifier():
    expected_categories = pd.array(
        ["Cat 0", "Cat 10"],
        dtype="string[pyarrow]",
    )
    expected = pd.Series(pd.Categorical(expected_categories, ordered=True))
    sr = pd.Series(["[0] Cat 0", "[10] Cat 10"], dtype=object)
    actual = object_to_str_categorical(sr, nr_identifiers=1, ordered=True)
    pd.testing.assert_series_equal(actual, expected)


def test_object_to_str_categorical_assert_renaming_2_identifier():
    expected_categories = pd.array(
        ["Cat 0", "Cat 10"],
        dtype="string[pyarrow]",
    )
    expected = pd.Series(pd.Categorical(expected_categories, ordered=True))
    sr = pd.Series(["[0] A Cat 0", "[10] B Cat 10"], dtype=object)
    actual = object_to_str_categorical(sr, nr_identifiers=2, ordered=True)
    pd.testing.assert_series_equal(actual, expected)


def test_object_to_int_categorical_assert_dtype():
    expected_categories = pd.array([0, 10], dtype="uint8[pyarrow]")
    expected = pd.Series(pd.Categorical(expected_categories)).dtype
    sr = pd.Series([0, 10], dtype=object)
    actual = object_to_int_categorical(sr).dtype
    assert actual == expected


def test_object_to_int_categorical_assert_ordering():
    expected_categories = pd.array([0, 10], dtype="uint8[pyarrow]")
    expected = pd.Series(pd.Categorical(expected_categories, ordered=True))
    sr = pd.Series([0, 10], dtype=object)
    actual = object_to_int_categorical(sr, ordered=True)
    pd.testing.assert_series_equal(actual, expected)
