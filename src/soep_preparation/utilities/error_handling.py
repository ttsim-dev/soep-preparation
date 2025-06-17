"""Error handling utilities for data validation."""

from collections.abc import Iterable
from typing import Any

import pandas as pd


def _fail_if_series_wrong_dtype(series: pd.Series, expected_dtype: str) -> None:
    if expected_dtype not in series.dtype.name and expected_dtype != "Any":
        msg = f"Expected dtype {expected_dtype}, got {series.dtype.name}"
        raise TypeError(msg)


def fail_if_input_has_invalid_type(input_: Any, expected_dtypes: Iterable[str]) -> None:  # noqa: ANN401
    """Fail if input is not of any of expected types.

    Args:
        input_: The input to check.
        expected_dtypes: The expected types of the input.

    Raises:
        TypeError: If input is not of any of expected types.
    """
    if (
        not any(
            expected_dtype in str(type(input_)) for expected_dtype in expected_dtypes
        )
        and "Any" not in expected_dtypes
    ):
        msg = f"Expected {input_} to be of type {expected_dtypes}, got {type(input_)}"
        raise TypeError(msg)


def fail_if_input_equals(input_: Any, failing_value: str) -> None:  # noqa: ANN401
    """Fail if input is equal to value.

    Args:
        input_: The input to check.
        failing_value: The value the input can not take.

    Raises:
        ValueError: If input is equal to value.
    """
    if input_ == failing_value:
        msg = f"Expected {input_} to be unequal to {failing_value}"
        raise ValueError(
            msg,
        )


def fail_if_series_cannot_be_transformed(
    series: pd.Series,
    expected_sr_dtype: str,
    input_expected_types: list[list] | None = None,
    entries_expected_types: list | None = None,
) -> None:
    """Check the dtype of a series and its entries.

    Args:
        series: The series to check.
        expected_sr_dtype: The expected dtype of the series.
        input_expected_types: A list of lists containing
            the inputs and their expected types. Defaults to None.
        entries_expected_types: A list of unique entries and
            their expected dtype. Defaults to None.

    Raises:
        TypeError: If series or its entries do not match expected types.
    """
    if input_expected_types is None:
        input_expected_types = [[]]
    _fail_if_series_wrong_dtype(series, expected_sr_dtype)
    if entries_expected_types is not None:
        type_ = entries_expected_types[1]
        for unique_entry in entries_expected_types[0]:
            fail_if_input_has_invalid_type(unique_entry, type_)
    else:
        msg = (
            "Did not receive a list of unique entries and their expected dtype, "
            "even though object dtype of series was specified."
        )
        raise Warning(
            msg,
        )
    for item in input_expected_types:
        fail_if_input_has_invalid_type(*item)


def fail_if_series_is_empty(series: pd.Series) -> None:
    """Fail if series is empty.

    Args:
        series: The series to check.

    Raises:
        ValueError: If series is empty.
    """
    if series.empty:
        msg = "Expected series to not be empty, but it was."
        raise ValueError(msg)


def fail_if_column_name_not_in_dataframe(
    dataframe: pd.DataFrame,
    column_name: str,
) -> None:
    """Fail if column name is not in DataFrame.

    Args:
        dataframe: The DataFrame to check.
        column_name: The column name to check.

    Raises:
        ValueError: If column name is not in DataFrame.
    """
    if column_name not in dataframe.columns:
        msg = f"Expected column '{column_name}' to be in DataFrame, but it was not."
        raise ValueError(msg)
