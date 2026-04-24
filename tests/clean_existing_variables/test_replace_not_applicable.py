import pandas as pd
import pytest

from soep_preparation.utilities.data_manipulator import replace_not_applicable_answer


@pytest.mark.parametrize(
    ("input_values", "expected_values"),
    [
        # Numeric -2 is replaced with 0
        ([-2, 100, 200], [0, 100, 200]),
        # String -2 label is replaced with 0
        (
            ["[-2] Trifft nicht zu", "[1] Ja", "[2] Nein"],
            [0, "[1] Ja", "[2] Nein"],
        ),
        # Other negative codes are left untouched
        ([-1, -3, -5, -8, 100], [-1, -3, -5, -8, 100]),
    ],
)
def test_replace_not_applicable_answer(input_values: list, expected_values: list):
    series = pd.Series(input_values, dtype=object)

    result = replace_not_applicable_answer(series=series, value=0)

    expected = pd.Series(expected_values, dtype=object)
    pd.testing.assert_series_equal(result, expected)
