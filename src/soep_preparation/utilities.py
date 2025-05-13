"""Utilities used in various parts of the project."""

import pandas as pd
from pandas.api.types import CategoricalDtype
from pytask import DataCatalog, PickleNode


def _fail_if_series_wrong_dtype(sr: pd.Series, expected_dtype: str):
    if expected_dtype not in sr.dtype.name:
        msg = f"Expected dtype {expected_dtype}, got {sr.dtype.name}"
        raise TypeError(msg)


def _fail_if_invalid_input(input_, expected_dtype: str):
    if expected_dtype not in str(type(input_)):
        msg = f"Expected {input_} to be of type {expected_dtype}, got {type(input_)}"
        raise TypeError(
            msg,
        )


def _fail_if_invalid_inputs(input_, expected_dtypes: str):
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
        _fail_if_invalid_input(input_, expected_dtypes)


def _error_handling_inputs(
    input_expected_types: list[list] | None = None,
):
    if input_expected_types is None:
        input_expected_types = [[]]
    [_fail_if_invalid_inputs(*item) for item in input_expected_types]


def _error_handling_object(
    sr: pd.Series,
    expected_sr_dtype: str,
    input_expected_types: list[list] | None = None,
    entries_expected_types: list | None = None,
):
    if input_expected_types is None:
        input_expected_types = [[]]
    _fail_if_series_wrong_dtype(sr, expected_sr_dtype)
    if entries_expected_types is not None:
        dtype = entries_expected_types[1]
        [
            _fail_if_invalid_inputs(unique_entry, dtype)
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
    [_fail_if_invalid_input(*item) for item in input_expected_types]


def apply_lowest_float_dtype(sr: pd.Series) -> pd.Series:
    """Apply the lowest integer dtype to a series."""
    return pd.to_numeric(sr, downcast="float", dtype_backend="pyarrow")


def apply_lowest_int_dtype(
    sr: pd.Series,
) -> pd.Series:
    """Apply the lowest integer dtype to a series."""
    try:
        return pd.to_numeric(sr, downcast="unsigned", dtype_backend="pyarrow")
    except ValueError:
        return pd.to_numeric(sr, downcast="integer", dtype_backend="pyarrow")


def find_lowest_int_dtype(sr: pd.Series) -> str:  # noqa: PLR0911
    """Find the lowest integer dtype for a series.

    Args:
        sr (pd.Series): The series to check.

    Returns:
        str: The lowest integer dtype.

    """
    if sr.isna().all():
        return "int64[pyarrow]"
    if "float" in sr.dtype.name:
        sr = sr.astype("float[pyarrow]")
    if sr.min() >= 0:
        if sr.max() <= 255:  # noqa: PLR2004
            return "uint8[pyarrow]"
        if sr.max() <= 65535:  # noqa: PLR2004
            return "uint16[pyarrow]"
        if sr.max() <= 4294967295:  # noqa: PLR2004
            return "uint32[pyarrow]"
        return "uint64[pyarrow]"
    if sr.min() >= -128 and sr.max() <= 127:  # noqa: PLR2004
        return "int8[pyarrow]"
    if sr.min() >= -32768 and sr.max() <= 32767:  # noqa: PLR2004
        return "int16[pyarrow]"
    if sr.min() >= -2147483648 and sr.max() <= 2147483647:  # noqa: PLR2004
        return "int32[pyarrow]"
    return "int64[pyarrow]"


def create_dummy(
    sr: pd.Series,
    true_value: bool | str | list,
    kind: str = "equality",
) -> pd.Series:
    """Create a dummy variable based on a condition.

    Args:
        sr (pd.Series): The input series to be transformed.
        true_value (bool | str | list): The value to be compared against.
        kind (str, optional): The type of comparison to be made. Defaults to "equality".
        Can be "equality", "neq", or "isin".

    Returns:
        pd.Series: A boolean series indicating the condition.
    """
    _error_handling_inputs(
        [
            [sr, "pandas.core.series.Series"],
            [true_value, "bool | str | list"],
            [kind, "str"],
        ],
    )
    if kind == "equality":
        return (sr == true_value).mask(sr.isna(), pd.NA).astype("bool[pyarrow]")
    if kind == "neq":
        return (sr != true_value).mask(sr.isna(), pd.NA).astype("bool[pyarrow]")
    if kind == "isin":
        return sr.isin(true_value).mask(sr.isna(), pd.NA).astype("bool[pyarrow]")
    msg = f"Unknown kind '{kind}' of dummy creation"
    raise ValueError(msg)


def float_to_int(
    sr: pd.Series,
    drop_missing: bool = False,  # noqa: FBT001, FBT002
) -> pd.Series:
    """Transform a float Series to an integer Series.

    Parameters:
        sr (pd.Series): The input series to be transformed.
        drop_missing (bool, optional): Whether to drop missing values.
          Defaults to False.

    Returns:
        pd.Series: The series with cleaned entries.
    """
    if drop_missing:
        sr_int = sr.astype("int")
        sr_no_missing = sr_int.where(sr_int >= 0, 0).replace({0: pd.NA})
        return apply_lowest_int_dtype(sr_no_missing)
    return apply_lowest_int_dtype(sr=sr)


def object_to_bool_categorical(
    sr: pd.Series,
    renaming: dict,
    ordered: bool = False,  # noqa: FBT001, FBT002
) -> pd.Series:
    """Transform a mixed object Series to a categorical bool Series.

    Parameters:
        sr (pd.Series): The input series to be cleaned.
        renaming (dict): A dictionary to rename the categories.
        ordered (bool, optional): Whether the categories should be returned as ordered.
        Defaults to False.

    Returns:
        pd.Series: The series with cleaned entries and transformed dtype.
    """
    _error_handling_object(
        sr,
        "object",
        [
            [sr, "pandas.core.series.Series"],
            [renaming, "dict"],
            [ordered, "bool"],
        ],
        [sr.unique(), "float | int | str"],
    )
    sr_relevant_values_only = _remove_missing_data_values(sr)

    sr_renamed = sr_relevant_values_only.replace(renaming)
    sr_bool = sr_renamed.astype("bool[pyarrow]")
    categories = pd.Series(data=pd.Series(renaming).unique(), dtype="bool[pyarrow]")

    raw_cat_dtype = CategoricalDtype(
        categories=categories,
        ordered=ordered,
    )
    return sr_bool.astype(raw_cat_dtype)


def object_to_float(sr: pd.Series) -> pd.Series:
    """Transform a mixed object Series to a float Series.

    Parameters:
        sr (pd.Series): The input series to be cleaned.

    Returns:
        pd.Series: The series with cleaned entries and transformed dtype.
    """
    _error_handling_object(
        sr,
        "object",
        [[sr, "pandas.core.series.Series"]],
        [sr.unique(), "float | int | str"],
    )
    sr_relevant_values_only = _remove_missing_data_values(sr)
    return apply_lowest_float_dtype(sr_relevant_values_only)


def object_to_int(sr: pd.Series) -> pd.Series:
    """Transform a mixed object Series to an integer Series.

    Parameters:
        sr (pd.Series): The input series to be cleaned.

    Returns:
        pd.Series: The series with cleaned entries and transformed dtype.
    """
    _error_handling_object(
        sr,
        "object",
        [[sr, "pandas.core.series.Series"]],
        [sr.unique(), "float | int | str"],
    )
    sr_relevant_values_only = _remove_missing_data_values(sr)
    return apply_lowest_int_dtype(sr_relevant_values_only)


def object_to_int_categorical(
    sr: pd.Series,
    renaming: dict | None = None,
    ordered: bool = False,  # noqa: FBT001, FBT002
) -> pd.Series:
    """Transform a mixed object Series to a categorical integer Series.

    Parameters:
        sr (pd.Series): The input series to be cleaned.
        renaming (dict | None, optional): A dictionary to rename the categories.
         Defaults to None.
        ordered (bool, optional): Whether the categories should be returned as ordered.
         Defaults to False.

    Returns:
        pd.Series: The series with cleaned entries and transformed dtype.
    """
    _error_handling_object(
        sr,
        "object",
        [
            [sr, "pandas.core.series.Series"],
            [renaming, "dict" if renaming is not None else "None"],
            [ordered, "bool"],
        ],
        [sr.unique(), "float | int | str"],
    )
    sr_relevant_values_only = _remove_missing_data_values(sr)
    if renaming:
        sr_renamed = sr_relevant_values_only.replace(renaming)
        sr_int = apply_lowest_int_dtype(sr_renamed)
        categories = apply_lowest_int_dtype(
            pd.Series(data=pd.Series(renaming).unique()),
        )
    else:
        sr_int = apply_lowest_int_dtype(sr_relevant_values_only)
        categories = apply_lowest_int_dtype(_get_sorted_not_na_unique_values(sr_int))

    raw_cat_dtype = CategoricalDtype(categories=categories, ordered=ordered)
    return sr_int.astype(raw_cat_dtype)


def object_to_str_categorical(
    sr: pd.Series,
    renaming: dict | None = None,
    ordered: bool = False,  # noqa: FBT001, FBT002
    nr_identifiers: int = 1,
) -> pd.Series:
    """Transform a mixed object Series to a categorical string Series.

    Parameters:
        sr (pd.Series): The input series to be cleaned.
        renaming (dict | None, optional): A dictionary to rename the categories.
         Defaults to None.
        ordered (bool, optional): Whether the categories should be returned
         as ordered. Defaults to False.categories. Defaults to False.
        nr_identifiers (int, optional): The number of identifiers inside
         each element to be removed. Defaults to 1.

    Returns:
        pd.Series: The series with cleaned entries and transformed dtype.
    """
    _error_handling_object(
        sr,
        "object",
        [
            [sr, "pandas.core.series.Series"],
            [renaming, "dict" if renaming is not None else "None"],
            [ordered, "bool"],
        ],
        [sr.unique(), "float | int | str"],
    )
    sr_relevant_values_only = _remove_missing_data_values(sr)
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


def _remove_missing_data_values(sr: pd.Series) -> pd.Series:
    """Remove values representing missing data or no response to the questionnaire.

    Parameters:
        sr (pd.Series): The pandas.Series to be manipulated.

    Returns:
        pd.Series: A new pd.Series with the missing data values replaced with NA.

    """
    values_to_remove = _get_values_to_remove(sr)
    return sr.replace(values_to_remove, pd.NA)


def _get_values_to_remove(sr: pd.Series) -> list:
    """Identify values representing missing data or no response in a pd.Series.

    Parameters:
        sr (pd.Series): The pandas.Series to analyze.

    Returns:
        list: A list of values to be treated as missing data.
    """
    unique_values = sr.unique()

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


def _get_sorted_not_na_unique_values(sr: pd.Series) -> pd.Series:
    unique_values = sr.unique()
    not_na_unique_values = unique_values[pd.notna(unique_values)]
    sorted_not_na_unique_values = sorted(not_na_unique_values)
    return pd.Series(sorted_not_na_unique_values)


def get_cleaned_and_potentially_merged_dataset(
    dataset_specific_datacatalog: dict[str, DataCatalog],
) -> PickleNode:
    """Get the cleaned and potentially merged with derived variables dataset.

    Args:
        dataset_specific_datacatalog (dict): The corresponding Datacatalog.

    Returns:
        pd.DataFrame: The cleaned and potentially merged dataset.
    """
    return (
        dataset_specific_datacatalog["merged"]
        if "merged" in dataset_specific_datacatalog._entries  # noqa: SLF001
        else dataset_specific_datacatalog["cleaned"]
    )
