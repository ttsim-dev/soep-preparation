import numpy as np
import pandas as pd
from soep_cleaning.initial_preprocessing.helper import (
    _remove_missing_data_categories,
)


def test_remove_missing_data_categories_check_dtype():
    expected = pd.Series(
        ["[0] Completely dissatisfied", "[10] Completely satisfied"],
        dtype="category",
    ).dtype
    sr = pd.Series(
        ["[0] Completely dissatisfied", "[10] Completely satisfied", np.nan],
        dtype="category",
    )
    actual = _remove_missing_data_categories(sr).dtype
    assert actual == expected
