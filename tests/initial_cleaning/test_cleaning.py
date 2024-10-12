import numpy as np
import pyarrow as pa

from soep_cleaning.config import pd
from soep_cleaning.utilities import (
    _remove_missing_data_categories,
    bool_categorical,
    int_to_int_categorical,
    str_categorical,
)


def test_remove_missing_data_categories_assert_dtype():
    expected_categories = pd.array(
        ["[0] Cat 0", "[10] Cat 10"],
        dtype=pd.ArrowDtype(pa.string()),
    )
    expected = pd.Series(pd.Categorical(expected_categories)).dtype
    sr = pd.Series(
        pd.Categorical(
            pd.array(
                ["[0] Cat 0", "[10] Cat 10", np.nan],
                dtype=pd.ArrowDtype(pa.string()),
            ),
        ),
    )
    actual = _remove_missing_data_categories(sr).dtype
    assert actual == expected


def test_remove_missing_data_categories_assert_values():
    expected_categories = pd.array(
        [pd.NA, pd.NA, "[0] Cat 0", "[10] Cat 10"],
        dtype=pd.ArrowDtype(pa.string()),
    )
    expected = pd.Series(pd.Categorical(expected_categories))
    sr = pd.Series(
        pd.Categorical(
            pd.array(
                ["[-9] Missing 1", "[-1] Missing 2", "[0] Cat 0", "[10] Cat 10"],
                dtype=pd.ArrowDtype(pa.string()),
            ),
        ),
    )
    actual = _remove_missing_data_categories(sr)
    pd.testing.assert_series_equal(actual, expected)


def test_bool_categorical_assert_dtype():
    expected_categories = pd.array([True, False], dtype=pd.ArrowDtype(pa.bool_()))
    expected = pd.Series(pd.Categorical(expected_categories)).dtype
    sr = pd.Series(
        pd.Categorical(pd.array(["Yes", "No"], dtype=pd.ArrowDtype(pa.string()))),
    )
    actual = bool_categorical(sr, {"Yes": True, "No": False}).dtype
    assert actual == expected


def test_bool_categorical_assert_renaming():
    expected_categories = pd.array([True, False], dtype=pd.ArrowDtype(pa.bool_()))
    expected = pd.Series(pd.Categorical(expected_categories))
    sr = pd.Series(
        pd.Categorical(pd.array(["Yes", "No"], dtype=pd.ArrowDtype(pa.string()))),
    )
    actual = bool_categorical(sr, {"No": False, "Yes": True})
    pd.testing.assert_series_equal(actual, expected)


def test_bool_categorical_assert_remove_missing():
    expected_categories = pd.array([True, False], dtype=pd.ArrowDtype(pa.bool_()))
    expected = pd.Series(
        pd.Categorical(expected_categories, ordered=True),
    ).cat.categories
    sr = pd.Series(
        pd.Categorical(
            pd.array(["Yes", "No", "[-1] Missing"], dtype=pd.ArrowDtype(pa.string())),
        ),
    )
    actual = bool_categorical(sr, {"No": False, "Yes": True}).cat.categories
    assert (actual == expected).all()


def test_str_categorical_assert_dtype():
    expected_categories = pd.array(
        ["Cat 0", "Cat 10"],
        dtype=pd.ArrowDtype(pa.string()),
    )
    expected = pd.Series(pd.Categorical(expected_categories, ordered=True)).dtype
    sr = pd.Series(
        pd.Categorical(
            pd.array(["[0] Cat 0", "[10] Cat 10"], dtype=pd.ArrowDtype(pa.string())),
        ),
    )
    actual = str_categorical(sr, nr_identifiers=1, ordered=True).dtype
    assert actual == expected


def test_str_categorical_assert_renaming_1_identifier():
    expected_categories = pd.array(
        ["Cat 0", "Cat 10"],
        dtype=pd.ArrowDtype(pa.string()),
    )
    expected = pd.Series(pd.Categorical(expected_categories, ordered=True))
    sr = pd.Series(
        pd.Categorical(
            pd.array(["[0] Cat 0", "[10] Cat 10"], dtype=pd.ArrowDtype(pa.string())),
        ),
    )
    actual = str_categorical(sr, nr_identifiers=1, ordered=True)
    pd.testing.assert_series_equal(actual, expected)


def test_str_categorical_assert_renaming_2_identifier():
    expected_categories = pd.array(
        ["Cat 0", "Cat 10"],
        dtype=pd.ArrowDtype(pa.string()),
    )
    expected = pd.Series(pd.Categorical(expected_categories, ordered=True))
    sr = pd.Series(
        pd.Categorical(
            pd.array(
                ["[0] A Cat 0", "[10] B Cat 10"],
                dtype=pd.ArrowDtype(pa.string()),
            ),
        ),
    )
    actual = str_categorical(sr, nr_identifiers=2, ordered=True)
    pd.testing.assert_series_equal(actual, expected)


def test_int__to_int_categorical_assert_dtype():
    expected_categories = pd.array([0, 10], dtype=pd.ArrowDtype(pa.uint8()))
    expected = pd.Series(pd.Categorical(expected_categories)).dtype
    sr = pd.Series([0, 10])
    actual = int_to_int_categorical(sr).dtype
    assert actual == expected


def test_int_to_int_categorical_assert_ordering():
    expected_categories = pd.array([0, 10], dtype=pd.ArrowDtype(pa.uint8()))
    expected = pd.Series(pd.Categorical(expected_categories, ordered=True))
    sr = pd.Series([0, 10])
    actual = int_to_int_categorical(sr, ordered=True)
    pd.testing.assert_series_equal(actual, expected)
