"""Utilities for manipulating pandas Series."""

import pandas as pd
from pandas.api.types import CategoricalDtype

from soep_preparation.utilities.error_handling import (
    error_handling_sr_transformation,
    fail_if_invalid_input,
    fail_if_invalid_inputs,
)


def _get_sorted_not_na_unique_values(series: pd.Series) -> pd.Series:
    unique_values = series.unique()
    not_na_unique_values = unique_values[pd.notna(unique_values)]
    sorted_not_na_unique_values = sorted(not_na_unique_values)
    return pd.Series(sorted_not_na_unique_values)


def _get_values_to_remove(series: pd.Series) -> list:
    """Identify values representing missing data or no response in a pd.Series.

    Parameters:
        series (pd.Series): The pandas.Series to analyze.

    Returns:
        list: A list of values to be treated as missing data.
    """
    unique_values = series.unique()

    str_values_to_remove = [
        value
        for value in unique_values
        if isinstance(value, str)
        and value.startswith("[-")
        and len(value) > 3  # noqa: PLR2004
        and value[3] == "]"
    ]
    num_values_to_remove = [
        value
        for value in unique_values
        if isinstance(value, (int | float)) and -9 < value < 0  # noqa: PLR2004
    ]

    return str_values_to_remove + num_values_to_remove


def _remove_missing_data_values(series: pd.Series) -> pd.Series:
    """Remove values representing missing data or no response to the questionnaire.

    Parameters:
        series (pd.Series): The pandas.Series to be manipulated.

    Returns:
        pd.Series: A new pd.Series with the missing data values replaced with NA.

    """
    values_to_remove = _get_values_to_remove(series)
    return series.replace(values_to_remove, pd.NA)


def apply_lowest_float_dtype(series: pd.Series) -> pd.Series:
    """Apply the lowest float dtype to a series.

    Args:
        series (pd.Series): The series to convert.

    Returns:
        pd.Series: The series with the lowest float dtype applied.
    """
    return pd.to_numeric(series, downcast="float", dtype_backend="pyarrow")


def apply_lowest_int_dtype(
    series: pd.Series,
) -> pd.Series:
    """Apply the lowest integer dtype to a series.

    Args:
        series (pd.Series): The series to convert.

    Returns:
        pd.Series: The series with the lowest integer dtype applied.
    """
    if not (series < 0).any():
        return pd.to_numeric(series, downcast="unsigned", dtype_backend="pyarrow")
    return pd.to_numeric(series, downcast="integer", dtype_backend="pyarrow")


def create_dummy(
    series: pd.Series,
    true_value: bool | str | list,
    kind: str = "equality",
) -> pd.Series:
    """Create a dummy variable based on a condition.

    Args:
        series (pd.Series): The input series to be transformed.
        true_value (bool | str | list): The value to be compared against.
        kind (str, optional): The type of comparison to be made. Defaults to "equality".
        Can be "equality", "neq", or "isin".

    Returns:
        pd.Series: A boolean series indicating the condition.
    """
    fail_if_invalid_input(series, "pandas.core.series.Series")
    fail_if_invalid_inputs(true_value, "bool | str | list")
    fail_if_invalid_input(kind, "str")
    if kind == "equality":
        return (series == true_value).mask(series.isna(), pd.NA).astype("bool[pyarrow]")
    if kind == "neq":
        return (series != true_value).mask(series.isna(), pd.NA).astype("bool[pyarrow]")
    if kind == "isin":
        return (
            series.isin(true_value).mask(series.isna(), pd.NA).astype("bool[pyarrow]")
        )
    msg = f"Unknown kind '{kind}' of dummy creation"
    raise ValueError(msg)


def find_lowest_int_dtype(series: pd.Series) -> str:  # noqa: PLR0911
    """Find the lowest integer dtype for a series.

    Args:
        series (pd.Series): The series to check.

    Returns:
        str: The lowest integer dtype.

    """
    if series.isna().all():
        return "int64[pyarrow]"
    if "float" in series.dtype.name:
        series = series.astype("float[pyarrow]")
    if series.min() >= 0:
        if series.max() <= 255:  # noqa: PLR2004
            return "uint8[pyarrow]"
        if series.max() <= 65535:  # noqa: PLR2004
            return "uint16[pyarrow]"
        if series.max() <= 4294967295:  # noqa: PLR2004
            return "uint32[pyarrow]"
        return "uint64[pyarrow]"
    if series.min() >= -128 and series.max() <= 127:  # noqa: PLR2004
        return "int8[pyarrow]"
    if series.min() >= -32768 and series.max() <= 32767:  # noqa: PLR2004
        return "int16[pyarrow]"
    if series.min() >= -2147483648 and series.max() <= 2147483647:  # noqa: PLR2004
        return "int32[pyarrow]"
    return "int64[pyarrow]"


def float_to_int(
    series: pd.Series,
    drop_missing: bool = False,  # noqa: FBT001, FBT002
) -> pd.Series:
    """Transform a float Series to an integer Series.

    Parameters:
        series (pd.Series): The input series to be transformed.
        drop_missing (bool, optional): Whether to drop missing values.
          Defaults to False.

    Returns:
        pd.Series: The series with cleaned entries.
    """
    if drop_missing:
        sr_int = series.astype("int")
        sr_no_missing = sr_int.where(sr_int >= 0, -1).replace({-1: pd.NA})
        return apply_lowest_int_dtype(sr_no_missing)
    return apply_lowest_int_dtype(series=series)


def object_to_float(series: pd.Series) -> pd.Series:
    """Transform a mixed object Series to a float Series.

    Parameters:
        series (pd.Series): The input series to be cleaned.

    Returns:
        pd.Series: The series with cleaned entries and transformed dtype.
    """
    error_handling_sr_transformation(
        series,
        "object",
        [[series, "pandas.core.series.Series"]],
        [series.unique(), "float | int | str"],
    )
    sr_relevant_values_only = _remove_missing_data_values(series)
    return apply_lowest_float_dtype(sr_relevant_values_only)


def object_to_int(series: pd.Series) -> pd.Series:
    """Transform a mixed object Series to an integer Series.

    Parameters:
        series (pd.Series): The input series to be cleaned.

    Returns:
        pd.Series: The series with cleaned entries and transformed dtype.
    """
    error_handling_sr_transformation(
        series,
        "object",
        [[series, "pandas.core.series.Series"]],
        [series.unique(), "float | int | str"],
    )
    sr_relevant_values_only = _remove_missing_data_values(series)
    return apply_lowest_int_dtype(sr_relevant_values_only)


def object_to_bool_categorical(
    series: pd.Series,
    renaming: dict,
    ordered: bool = False,  # noqa: FBT001, FBT002
) -> pd.Series:
    """Transform a mixed object Series to a categorical bool Series.

    Parameters:
        series (pd.Series): The input series to be cleaned.
        renaming (dict): A dictionary to rename the categories.
        ordered (bool, optional): Whether the categories should be returned as ordered.
        Defaults to False.

    Returns:
        pd.Series: The series with cleaned entries and transformed dtype.
    """
    error_handling_sr_transformation(
        series,
        "object",
        [
            [series, "pandas.core.series.Series"],
            [renaming, "dict"],
            [ordered, "bool"],
        ],
        [series.unique(), "float | int | str"],
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
    ordered: bool = False,  # noqa: FBT001, FBT002
) -> pd.Series:
    """Transform a mixed object Series to a categorical integer Series.

    Parameters:
        series (pd.Series): The input series to be cleaned.
        renaming (dict | None, optional): A dictionary to rename the categories.
         Defaults to None.
        ordered (bool, optional): Whether the categories should be returned as ordered.
         Defaults to False.

    Returns:
        pd.Series: The series with cleaned entries and transformed dtype.
    """
    error_handling_sr_transformation(
        series,
        "object",
        [
            [series, "pandas.core.series.Series"],
            [renaming, "dict" if renaming is not None else "None"],
            [ordered, "bool"],
        ],
        [series.unique(), "float | int | str"],
    )
    sr_relevant_values_only = _remove_missing_data_values(series)
    if renaming:
        sr_renamed = sr_relevant_values_only.replace(renaming)
        sr_int = apply_lowest_int_dtype(sr_renamed)
        categories = apply_lowest_int_dtype(
            pd.Series(
                data=pd.Series(renaming).unique(),
                dtype=find_lowest_int_dtype(sr_int),
            ),
        )
    else:
        sr_int = apply_lowest_int_dtype(sr_relevant_values_only)
        categories = apply_lowest_int_dtype(_get_sorted_not_na_unique_values(sr_int))

    raw_cat_dtype = CategoricalDtype(categories=categories, ordered=ordered)
    return sr_int.astype(raw_cat_dtype)


def object_to_str_categorical(
    series: pd.Series,
    renaming: dict | None = None,
    ordered: bool = False,  # noqa: FBT001, FBT002
    nr_identifiers: int = 1,
) -> pd.Series:
    """Transform a mixed object Series to a categorical string Series.

    Parameters:
        series (pd.Series): The input series to be cleaned.
        renaming (dict | None, optional): A dictionary to rename the categories.
         Defaults to None.
        ordered (bool, optional): Whether the categories should be returned
         as ordered. Defaults to False.categories. Defaults to False.
        nr_identifiers (int, optional): The number of identifiers inside
         each element to be removed. Defaults to 1.

    Returns:
        pd.Series: The series with cleaned entries and transformed dtype.
    """
    error_handling_sr_transformation(
        series,
        "object",
        [
            [series, "pandas.core.series.Series"],
            [renaming, "dict" if renaming is not None else "None"],
            [ordered, "bool"],
        ],
        [series.unique(), "float | int | str"],
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
