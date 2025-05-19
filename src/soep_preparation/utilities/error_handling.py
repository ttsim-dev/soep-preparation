"""Error handling utilities for data validation."""

from typing import Any

import pandas as pd


def _fail_if_series_wrong_dtype(series: pd.Series, expected_dtype: str) -> None:
    if expected_dtype not in series.dtype.name:
        msg = f"Expected dtype {expected_dtype}, got {series.dtype.name}"
        raise TypeError(msg)


def fail_if_invalid_input(input_: Any, expected_dtype: str) -> None:  # noqa: ANN401
    """Fail if the input is not of the expected type.

    Args:
        input_ (Any): The input to check.
        expected_dtype (str): The expected type of the input.

    Raises:
        TypeError: If the input is not of the expected type.
    """
    if expected_dtype not in str(type(input_)):
        msg = f"Expected {input_} to be of type {expected_dtype}, got {type(input_)}"
        raise TypeError(
            msg,
        )


def fail_if_invalid_inputs(input_: Any, expected_dtypes: str) -> None:  # noqa: ANN401
    """Fail if the input is not of any of the expected types.

    Args:
        input_ (Any): The input to check.
        expected_dtypes (str): The expected types of the input.

    Raises:
        TypeError: If the input is not of any of the expected types.
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
        fail_if_invalid_input(input_, expected_dtypes)


def error_handling_sr_transformation(
    series: pd.Series,
    expected_sr_dtype: str,
    input_expected_types: list[list] | None = None,
    entries_expected_types: list | None = None,
) -> None:
    """Check the dtype of a series and its entries.

    Args:
        series (pd.Series): The series to check.
        expected_sr_dtype (str): The expected dtype of the series.
        input_expected_types (list[list], optional): A list of lists containing
            the inputs and their expected types. Defaults to None.
        entries_expected_types (list, optional): A list of unique entries and
            their expected dtype. Defaults to None.

    Raises:
        TypeError: If the series or its entries do not match the expected types.
    """
    if input_expected_types is None:
        input_expected_types = [[]]
    _fail_if_series_wrong_dtype(series, expected_sr_dtype)
    if entries_expected_types is not None:
        dtype = entries_expected_types[1]
        [
            fail_if_invalid_inputs(unique_entry, dtype)
            for unique_entry in entries_expected_types[0]
        ]
    else:
        msg = (
            "Did not receive a list of unique entries and their expected dtype, "
            "even though object dtype of series was specified."
        )
        raise Warning(
            msg,
        )
    [fail_if_invalid_input(*item) for item in input_expected_types]
