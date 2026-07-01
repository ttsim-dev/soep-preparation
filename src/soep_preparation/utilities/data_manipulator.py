"""Utilities for manipulating data."""

import re
from collections.abc import Mapping

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

    # negative single digit values (-1 through -9) represent missing data

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
        if isinstance(value, (int | float)) and -10 < value < 0  # noqa: PLR2004
    ]

    return str_values_to_remove + num_values_to_remove


def replace_not_applicable_answer(series: pd.Series, value: float) -> pd.Series:
    """Replace "does not apply" (-2) SOEP codes with a concrete value.

    Code -2 ("trifft nicht zu") indicates that the question was not applicable
    to the respondent.  For income variables this means "no income from this
    source", so the appropriate replacement is 0.

    All other negative codes (-1, -3 … -9) represent genuinely missing data
    and are left unchanged for ``replace_missing_codes_with_na`` to convert to
    NA.

    Parameters:
        series: The input series.
        value: The replacement value for "does not apply".

    Returns:
        A new series with -2 codes replaced.
    """
    unique_values = series.unique()
    not_applicable_str = re.compile(r"\[-2\]\s.+")
    replacements: dict = {
        v: value
        for v in unique_values
        if (isinstance(v, (int | float)) and v == -2)  # noqa: PLR2004
        or (isinstance(v, str) and not_applicable_str.match(v))
    }
    if not replacements:
        return series
    return series.replace(replacements)


def replace_missing_codes_with_na(series: pd.Series) -> pd.Series:
    """Replace SOEP missing-data codes with NA.

    SOEP encodes missing data and "no response" in two forms, both handled here:

    - numeric single-digit codes (-1 through -9)
    - labelled strings of the form ``[-N] ...`` (e.g. ``[-1] Missing``)

    Parameters:
        series: The series to be cleaned.

    Returns:
        A new series with all missing-data codes replaced by NA.

    """
    values_to_remove = _get_na_values_to_remove(series)
    return series.replace(values_to_remove, pd.NA)


def convert_to_float(series: pd.Series) -> pd.Series:
    """Convert a numeric or numeric-like series to 64-bit pyarrow float.

    Float columns are always stored as `float64[pyarrow]`; unlike integers, floats
    are not downcast to the smallest dtype.

    Args:
        series: The series to convert.

    Returns:
        The series as `float64[pyarrow]`.
    """
    return pd.to_numeric(series, dtype_backend="pyarrow").astype("float64[pyarrow]")


def apply_smallest_int_dtype(
    series: pd.Series,
) -> pd.Series:
    """Apply the smallest bit-size integer dtype to a series.

    Args:
        series: The series to convert.

    Returns:
        The series with the smallest integer dtype applied.
    """
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
        series=series,
        expected_sr_dtype="Any",
        input_expected_types=[
            [series, "pandas.core.series.Series"],
            [ordered, "bool"],
        ],
        entries_expected_types=[series.unique(), ["Any"]],
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
    fail_if_input_has_invalid_type(input_=series, expected_dtypes=["pandas.Series"])
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
    code_negative_values_as_na: bool,
) -> pd.Series:
    """Transform a float Series to an integer Series.

    Parameters:
        series: The input series to be transformed.
        code_negative_values_as_na: Code negative values as NA if True.

    Returns:
        The series with cleaned entries.
    """
    if code_negative_values_as_na:
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
        series=series,
        expected_sr_dtype="object",
        input_expected_types=[[series, "pandas.core.series.Series"]],
        entries_expected_types=[series.unique(), ("float", "int", "str")],
    )
    sr_relevant_values_only = replace_missing_codes_with_na(series)
    return convert_to_float(sr_relevant_values_only)


def object_to_int(series: pd.Series, renaming: dict | None = None) -> pd.Series:
    """Transform a mixed object Series to a plain integer Series.

    Use this for variables whose integer *is* the value — month numbers and genuine
    rating scales — including ones whose raw labels need remapping to codes (pass
    `renaming`). The result is a plain `int[pyarrow]` column, so missing values
    display as `<NA>` rather than the numpy `NaN` a `category` dtype would show.

    Parameters:
        series: The input series to be cleaned.
        renaming: Optional mapping from raw labels to integer codes. Defaults to None.

    Returns:
        The series with cleaned entries and transformed dtype.
    """
    fail_if_series_cannot_be_transformed(
        series=series,
        expected_sr_dtype="object",
        input_expected_types=[
            [series, "pandas.core.series.Series"],
            [renaming, "dict" if renaming is not None else "None"],
        ],
        entries_expected_types=[series.unique(), ("float", "int", "str")],
    )
    sr_relevant_values_only = replace_missing_codes_with_na(series)
    if renaming:
        sr_relevant_values_only = sr_relevant_values_only.replace(renaming)
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
        series=series,
        expected_sr_dtype="object",
        input_expected_types=[
            [series, "pandas.core.series.Series"],
            [renaming, "dict"],
            [ordered, "bool"],
        ],
        entries_expected_types=[series.unique(), ("float", "int", "str")],
    )
    sr_relevant_values_only = replace_missing_codes_with_na(series)

    sr_renamed = sr_relevant_values_only.replace(renaming)
    sr_bool = sr_renamed.astype("bool[pyarrow]")
    categories = pd.Series(data=pd.Series(renaming).unique(), dtype="bool[pyarrow]")

    raw_cat_dtype = CategoricalDtype(
        categories=categories,
        ordered=ordered,
    )
    return sr_bool.astype(raw_cat_dtype)


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
        series=series,
        expected_sr_dtype="object",
        input_expected_types=[
            [series, "pandas.core.series.Series"],
            [renaming, "dict" if renaming is not None else "None"],
            [ordered, "bool"],
        ],
        entries_expected_types=[series.unique(), ("float", "int", "str")],
    )
    sr_relevant_values_only = replace_missing_codes_with_na(series)
    if renaming:
        sr_renamed = sr_relevant_values_only.replace(renaming)
        sr_str = sr_renamed.astype("string[pyarrow]")
        categories = pd.Series(
            data=pd.Series(renaming).unique(), dtype="string[pyarrow]"
        )
    else:
        sr_renamed = sr_relevant_values_only.str.split(pat=" ", n=nr_identifiers).str[
            -1
        ]
        sr_str = sr_renamed.astype("string[pyarrow]")
        categories = _get_sorted_not_na_unique_values(sr_str).astype("string[pyarrow]")

    raw_cat_dtype = CategoricalDtype(
        categories=categories,
        ordered=ordered,
    )
    return sr_str.astype(raw_cat_dtype)


def translate_categories(
    series: pd.Series,
    translations: Mapping[str, str],
) -> pd.Series:
    """Relabel a categorical Series' categories from German to English.

    Every current category must be a key in `translations`, so an unmapped or newly
    appearing category fails loudly instead of silently staying German (`translations`
    may over-cover with keys absent from the data). The result keeps pyarrow-string
    categories, re-sorted English-alphabetically for a stable, consistent order.

    Args:
        series: A categorical Series whose categories are the German labels.
        translations: Map from each German category to its English label.

    Returns:
        The Series with English categories.

    Raises:
        ValueError: If any current category is missing from `translations`.
    """
    _fail_if_categories_not_translated(series, translations)
    relabelled = series.astype("string[pyarrow]").replace(dict(translations))
    categories = _get_sorted_not_na_unique_values(relabelled).astype("string[pyarrow]")
    raw_cat_dtype = CategoricalDtype(categories=categories, ordered=series.cat.ordered)
    return relabelled.astype(raw_cat_dtype)


def _fail_if_categories_not_translated(
    series: pd.Series,
    translations: Mapping[str, str],
) -> None:
    missing = [
        category for category in series.cat.categories if category not in translations
    ]
    if missing:
        msg = f"categories missing from the translation map: {missing}"
        raise ValueError(msg)


def combined_categorical(
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


def combined_int(series_1: pd.Series, series_2: pd.Series) -> pd.Series:
    """Combine two integer series, preferring the first, as a plain integer Series.

    Args:
        series_1: The first series; its values win where both are present.
        series_2: The second series; fills in where the first is missing.

    Returns:
        The combined `int[pyarrow]` series, so missings display as `<NA>`.
    """
    fail_if_series_is_empty(series_1)
    fail_if_series_is_empty(series_2)
    combined = series_1.combine_first(series_2)
    return apply_smallest_int_dtype(combined)
