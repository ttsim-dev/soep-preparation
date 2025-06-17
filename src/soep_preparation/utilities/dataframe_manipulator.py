"""Utilities for DataFrame manipulation."""

import pandas as pd

from soep_preparation.utilities.error_handling import (
    fail_if_column_name_not_in_dataframe,
    fail_if_series_is_empty,
)
from soep_preparation.utilities.series_manipulator import convert_to_categorical


def combine_first_and_make_categorical(
    data: pd.DataFrame,
    column_name_1: str,
    column_name_2: str,
    ordered: bool,  # noqa: FBT001
) -> pd.Series:
    """Combine two columns and convert to categorical.

    Args:
        data: Data containing columns.
        column_name_1: The first column name.
        column_name_2: The second column name.
        ordered: Whether the categorical is ordered.

    Returns:
        The combined and converted categorical series.
    """
    fail_if_column_name_not_in_dataframe(data, column_name_1)
    fail_if_series_is_empty(data[column_name_1])
    fail_if_column_name_not_in_dataframe(data, column_name_2)
    fail_if_series_is_empty(data[column_name_2])
    combined = data[column_name_1].combine_first(data[column_name_2])
    return convert_to_categorical(combined, ordered=ordered)
