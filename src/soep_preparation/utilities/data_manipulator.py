"""Utilities for manipulating data."""

import re

import pandas as pd
from pandas.api.types import CategoricalDtype

from soep_preparation.utilities.error_handling import (
    fail_if_input_equals,
    fail_if_input_has_invalid_type,
    fail_if_series_cannot_be_transformed,
    fail_if_series_is_empty,
)


def _get_sorted_not_na_unique_values(series: pd.Series) -> pd.Series:
    unique_values = series.unique()
    not_na_unique_values = unique_values[pd.notna(unique_values)]
    sorted_not_na_unique_values = sorted(not_na_unique_values)
    return pd.Series(sorted_not_na_unique_values)


def _get_na_values_to_remove(series: pd.Series) -> list:
    """Identify values representing missing data or no response in a Series.

    Parameters:
        series: The Series to analyze.

    Returns:
        A list of values to be treated as missing data.
    """
    unique_values = series.unique()

    # negative single digit values (-1 through -8) represent missing data

    # strings representing missing data have the following pattern:
    # e.g. "[-1] Potentially some missing name"
    pattern = re.compile(r"\[-\d\]\s.+")
    str_values_to_remove = [
        value
        for value in unique_values
        if isinstance(value, str) and pattern.match(value)
    ]
    # numerical missing data values
    num_values_to_remove = [
        value
        for value in unique_values
        if isinstance(value, (int | float)) and -9 < value < 0  # noqa: PLR2004
    ]

    return str_values_to_remove + num_values_to_remove


def _remove_missing_data_values(series: pd.Series) -> pd.Series:
    """Remove values representing missing data or no response to the questionnaire.

    Parameters:
        series: The series to be manipulated.

    Returns:
        A new series with the missing data values replaced with NA.

    """
    values_to_remove = _get_na_values_to_remove(series)
    return series.replace(values_to_remove, pd.NA)


def apply_smallest_float_dtype(series: pd.Series) -> pd.Series:
    """Apply the smallest bit-size float dtype to a series.

    Args:
        series: The series to convert.

    Returns:
        The series with the smallest float dtype applied.
    """
    return pd.to_numeric(series, downcast="float", dtype_backend="pyarrow")


def apply_smallest_int_dtype(
    series: pd.Series,
) -> pd.Series:
    """Apply the smallest bit-size integer dtype to a series.

    Args:
        series: The series to convert.

    Returns:
        The series with the smallest integer dtype applied.
    """
    if not (series < 0).any():
        return pd.to_numeric(series, downcast="unsigned", dtype_backend="pyarrow")
    return pd.to_numeric(series, downcast="integer", dtype_backend="pyarrow")


def convert_to_categorical(
    series: pd.Series,
    ordered: bool,
) -> pd.Series:
    """Convert a series to a categorical series.

    Args:
        series: The series to convert.
        ordered: Whether the categories should be returned as ordered.

    Returns:
        The series converted to categorical dtype.
    """
    fail_if_series_cannot_be_transformed(
        series,
        "Any",
        [
            [series, "pandas.core.series.Series"],
            [ordered, "bool"],
        ],
        [series.unique(), ["Any"]],
    )
    if series.isna().all():
        return series.astype("category[pyarrow]")
    categories = _get_sorted_not_na_unique_values(series)
    raw_cat_dtype = CategoricalDtype(
        categories=categories,
        ordered=ordered,
    )
    return series.astype(raw_cat_dtype)


def create_dummy(
    series: pd.Series,
    value_for_comparison: bool | str | list | float,
    comparison_type: str = "equal",
) -> pd.Series:
    """Create a dummy variable based on a condition.

    Args:
        series: The input series to be transformed.
        value_for_comparison: The value to be compared against.
        comparison_type: The type of comparison to be made. Defaults to "equal".
        Can be "equal", "geq", "isin", "startswith", "leq" or "neq".

    Returns:
        A boolean series indicating the condition.
    """
    fail_if_input_has_invalid_type(
        input_=series, expected_dtypes=["pandas.core.series.Series"]
    )
    if type(value_for_comparison) is not str:
        fail_if_input_equals(input_=comparison_type, failing_value="startswith")
    fail_if_input_has_invalid_type(
        input_=value_for_comparison,
        expected_dtypes=("bool", "str", "list", "float", "int"),
    )
    fail_if_input_has_invalid_type(comparison_type, expected_dtypes=["str"])
    if comparison_type == "equal":
        return (
            (series == value_for_comparison)
            .mask(series.isna(), pd.NA)
            .astype("bool[pyarrow]")
        )
    if comparison_type == "neq":
        return (
            (series != value_for_comparison)
            .mask(series.isna(), pd.NA)
            .astype("bool[pyarrow]")
        )
    if comparison_type == "isin":
        return (
            series.isin(value_for_comparison)
            .mask(series.isna(), pd.NA)
            .astype("bool[pyarrow]")
        )
    if comparison_type == "geq":
        return (
            series.ge(value_for_comparison)
            .mask(series.isna(), pd.NA)
            .astype("bool[pyarrow]")
        )
    if comparison_type == "leq":
        return (
            series.le(value_for_comparison)
            .mask(series.isna(), pd.NA)
            .astype("bool[pyarrow]")
        )
    if comparison_type == "startswith":
        return (
            series.str.startswith(value_for_comparison)
            .mask(series.isna(), pd.NA)
            .astype(
                "bool[pyarrow]",
            )
        )
    msg = f"Unknown comparison type '{comparison_type}' of dummy creation"
    raise ValueError(msg)


def float_to_int(
    series: pd.Series,
    drop_missing: bool = False,  # noqa: FBT002
) -> pd.Series:
    """Transform a float Series to an integer Series.

    Parameters:
        series: The input series to be transformed.
        drop_missing: Whether to drop missing values.
          Defaults to False.

    Returns:
        The series with cleaned entries.
    """
    if drop_missing:
        sr_int = series.astype("int")
        sr_no_missing = sr_int.where(sr_int >= 0, -1).replace({-1: pd.NA})
        return apply_smallest_int_dtype(sr_no_missing)
    return apply_smallest_int_dtype(series=series)


def object_to_float(series: pd.Series) -> pd.Series:
    """Transform a mixed object Series to a float Series.

    Parameters:
        series: The input series to be cleaned.

    Returns:
        The series with cleaned entries and transformed dtype.
    """
    fail_if_series_cannot_be_transformed(
        series,
        "object",
        [[series, "pandas.core.series.Series"]],
        [series.unique(), ("float", "int", "str")],
    )
    sr_relevant_values_only = _remove_missing_data_values(series)
    return apply_smallest_float_dtype(sr_relevant_values_only)


def object_to_int(series: pd.Series) -> pd.Series:
    """Transform a mixed object Series to an integer Series.

    Parameters:
        series: The input series to be cleaned.

    Returns:
        The series with cleaned entries and transformed dtype.
    """
    fail_if_series_cannot_be_transformed(
        series,
        "object",
        [[series, "pandas.core.series.Series"]],
        [series.unique(), ("float", "int", "str")],
    )
    sr_relevant_values_only = _remove_missing_data_values(series)
    return apply_smallest_int_dtype(sr_relevant_values_only)


def object_to_bool_categorical(
    series: pd.Series,
    renaming: dict,
    ordered: bool = False,  # noqa: FBT002
) -> pd.Series:
    """Transform a mixed object Series to a categorical bool Series.

    Parameters:
        series: The input series to be cleaned.
        renaming: A dictionary to rename the categories.
        ordered: Whether the categories should be returned as ordered.
        Defaults to False.

    Returns:
        The series with cleaned entries and transformed dtype.
    """
    fail_if_series_cannot_be_transformed(
        series,
        "object",
        [
            [series, "pandas.core.series.Series"],
            [renaming, "dict"],
            [ordered, "bool"],
        ],
        [series.unique(), ("float", "int", "str")],
    )
    sr_relevant_values_only = _remove_missing_data_values(series)

    sr_renamed = sr_relevant_values_only.replace(renaming)
    sr_bool = sr_renamed.astype("bool[pyarrow]")
    categories = pd.Series(data=pd.Series(renaming).unique(), dtype="bool[pyarrow]")

    raw_cat_dtype = CategoricalDtype(
        categories=categories,
        ordered=ordered,
    )
    return sr_bool.astype(raw_cat_dtype)


def object_to_int_categorical(
    series: pd.Series,
    renaming: dict | None = None,
    ordered: bool = False,  # noqa: FBT002
) -> pd.Series:
    """Transform a mixed object Series to a categorical integer Series.

    Parameters:
        series: The input series to be cleaned.
        renaming: A dictionary to rename the categories.
         Defaults to None.
        ordered: Whether the categories should be returned as ordered.
         Defaults to False.

    Returns:
        The series with cleaned entries and transformed dtype.
    """
    fail_if_series_cannot_be_transformed(
        series,
        "object",
        [
            [series, "pandas.core.series.Series"],
            [renaming, "dict" if renaming is not None else "None"],
            [ordered, "bool"],
        ],
        [series.unique(), ("float", "int", "str")],
    )
    sr_relevant_values_only = _remove_missing_data_values(series)
    if renaming:
        sr_renamed = sr_relevant_values_only.replace(renaming)
        sr_int = apply_smallest_int_dtype(sr_renamed)
        categories = apply_smallest_int_dtype(
            pd.Series(
                data=pd.Series(renaming).unique(),
                dtype=apply_smallest_int_dtype(sr_int).dtype,
            ),
        )
    else:
        sr_int = apply_smallest_int_dtype(sr_relevant_values_only)
        categories = apply_smallest_int_dtype(_get_sorted_not_na_unique_values(sr_int))

    raw_cat_dtype = CategoricalDtype(categories=categories, ordered=ordered)
    return sr_int.astype(raw_cat_dtype)


def object_to_str_categorical(
    series: pd.Series,
    renaming: dict | None = None,
    ordered: bool = False,  # noqa: FBT002
    nr_identifiers: int = 1,
) -> pd.Series:
    """Transform a mixed object Series to a categorical string Series.

    Parameters:
        series: The input series to be cleaned.
        renaming: A dictionary to rename the categories.
         Defaults to None.
        ordered: Whether the categories should be returned
         as ordered. Defaults to False.categories. Defaults to False.
        nr_identifiers: The number of identifiers inside
         each element to be removed. Defaults to 1.

    Returns:
        The series with cleaned entries and transformed dtype.
    """
    fail_if_series_cannot_be_transformed(
        series,
        "object",
        [
            [series, "pandas.core.series.Series"],
            [renaming, "dict" if renaming is not None else "None"],
            [ordered, "bool"],
        ],
        [series.unique(), ("float", "int", "str")],
    )
    sr_relevant_values_only = _remove_missing_data_values(series)
    if renaming:
        sr_renamed = sr_relevant_values_only.replace(renaming)
        sr_str = sr_renamed.astype("str[pyarrow]")
        categories = pd.Series(data=pd.Series(renaming).unique(), dtype="str[pyarrow]")
    else:
        sr_renamed = sr_relevant_values_only.str.split(pat=" ", n=nr_identifiers).str[
            -1
        ]
        sr_str = sr_renamed.astype("str[pyarrow]")
        categories = _get_sorted_not_na_unique_values(sr_str).astype("str[pyarrow]")

    raw_cat_dtype = CategoricalDtype(
        categories=categories,
        ordered=ordered,
    )
    return sr_str.astype(raw_cat_dtype)


def combine_first_and_make_categorical(
    series_1: pd.Series,
    series_2: pd.Series,
    ordered: bool,
) -> pd.Series:
    """Combine two series and convert to categorical.

    Args:
        series_1: The first series.
        series_2: The second series.
        ordered: Whether the categorical is ordered.

    Returns:
        The combined and converted categorical series.
    """
    fail_if_series_is_empty(series_1)
    fail_if_series_is_empty(series_2)
    combined = series_1.combine_first(series_2)
    return convert_to_categorical(combined, ordered=ordered)
