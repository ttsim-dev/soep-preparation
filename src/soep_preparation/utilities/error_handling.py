"""Error handling utilities for data validation."""

from typing import Any

import pandas as pd


def _fail_if_series_wrong_dtype(series: pd.Series, expected_dtype: str) -> None:
    if expected_dtype not in series.dtype.name and expected_dtype != "Any":
        msg = f"Expected dtype {expected_dtype}, got {series.dtype.name}"
        raise TypeError(msg)


def fail_if_input_invalid_type(input_: Any, expected_dtype: str) -> None:
    """Fail if input is not of expected type.

    Args:
        input_: The input to check.
        expected_dtype: The expected type of the input.

    Raises:
        TypeError: If input is not of expected type.
    """
    if expected_dtype not in str(type(input_)) and expected_dtype != "Any":
        msg = f"Expected {input_} to be of type {expected_dtype}, got {type(input_)}"
        raise TypeError(
            msg,
        )


def fail_if_input_all_invalid_types(input_: Any, expected_dtypes: str) -> None:
    """Fail if input is not of any of expected types.

    Args:
        input_: The input to check.
        expected_dtypes: The expected types of the input.

    Raises:
        TypeError: If input is not of any of expected types.
    """
    if " | " in expected_dtypes:
        if not any(
            expected_dtype in str(type(input_))
            for expected_dtype in expected_dtypes.split(" | ")
        ):
            msg = (
                f"Expected {input_} to be of type {expected_dtypes}, got {type(input_)}"
            )
            raise TypeError(
                msg,
            )

    else:
        fail_if_input_invalid_type(input_, expected_dtypes)


def fail_if_input_equals(input_: Any, failing_value: str) -> None:
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
            fail_if_input_all_invalid_types(unique_entry, type_)
    else:
        msg = (
            "Did not receive a list of unique entries and their expected dtype, "
            "even though object dtype of series was specified."
        )
        raise Warning(
            msg,
        )
    for item in input_expected_types:
        fail_if_input_invalid_type(*item)
