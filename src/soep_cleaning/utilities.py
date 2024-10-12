"""Utilities used in various parts of the project."""

import numpy as np

from soep_cleaning.config import pd


def _fail_if_series_wrong_dtype(sr: pd.Series, expected_dtype: str):
    if expected_dtype not in sr.dtype.name:
        msg = f"Expected dtype {expected_dtype}, got {sr.dtype.name}"
        raise TypeError(msg)


def _fail_series_without_categories(sr: pd.Series):
    if sr.cat.categories is None:
        msg = "Series has no categories"
        raise ValueError(msg)


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


def _error_handling_categorical(
    sr: pd.Series,
    expected_sr_dtype: str,
    input_expected_types: list[list] | None = None,
    categories_expected_types: list | None = None,
):
    if input_expected_types is None:
        input_expected_types = [[]]
    _fail_if_series_wrong_dtype(sr, expected_sr_dtype)
    if expected_sr_dtype == "category":
        _fail_series_without_categories(sr)
        if categories_expected_types is not None:
            dtype = categories_expected_types[1]
            [
                _fail_if_invalid_inputs(category, dtype)
                for category in categories_expected_types[0]
            ]
        else:
            msg = "Did not receive a list of categories and their expected dtype, even though categorical dtype of series was specified."
            raise Warning(
                msg,
            )
    [_fail_if_invalid_input(*item) for item in input_expected_types]


def _remove_missing_data_categories(sr: pd.Series) -> pd.Series:
    """Remove categories representing missing data or no response to the questionnaire
    from a categorical Series.

    Parameters:
        sr (pd.Series): A pandas Series of dtype 'category' containing the categorical data.

    Returns:
        pd.Series: A new categorical Series with the missing data categories removed.

    """
    str_categories = sr.cat.categories[
        sr.cat.categories.map(lambda x: isinstance(x, str))
    ]
    removing_categories = [
        i for i in str_categories if i.startswith("[-") and i[3] == "]"
    ]
    num_categories = [
        value
        for value in sr.cat.categories
        if isinstance(value, int | float | np.integer | np.floating)
    ]
    removing_categories += [cat for cat in num_categories if cat < 0]
    return sr.cat.set_categories(sr.cat.categories.drop(removing_categories))


def _reduce_categories(sr, renaming, ordered, categories_type_str="str[pyarrow]"):
    sr_renamed = sr.astype(categories_type_str).replace(renaming)
    if "int" in categories_type_str:
        categories_type_str = find_lowest_int_dtype(sr_renamed)
    elif "bool" in categories_type_str:
        categories_type_str = "bool[pyarrow]"
    category_dtype = pd.CategoricalDtype(
        categories=sr_renamed.dropna().unique().astype(categories_type_str),
        ordered=ordered,
    )
    return pd.Series(sr_renamed, dtype=category_dtype)


def _renaming_categories(sr, renaming, ordered, categories_type_str="str[pyarrow]"):
    """Rename the categories of a pd.Series of dtype category based on the renaming
    dict.

    Parameters:
        sr (pd.Series): A pandas Series of dtype 'category' containing the categorical data.
        renaming (dict): A dictionary to rename the categories.
        ordered (bool): Whether the series should be returned as ordered, order imputed from renaming keys.
        categories_type_str (str,optional): DataType of the renaming keys. Defaults to str[pyarrow].

    Return:
        pd.Series: A new categorical Series with the renamed categories.

    """
    # TODO: check if _set_new_categories() is adequate
    """
    category_dtype = pd.CategoricalDtype(
        sr_renamed.cat.categories.astype(categories_type_str),
        ordered,
    )
    return pd.Series(sr_renamed, dtype=category_dtype)
    """
    sr_renamed = sr.cat.reorder_categories(
        renaming,
        ordered=ordered,
    ).cat.rename_categories(renaming)

    if "int" in categories_type_str:
        categories_type_str = find_lowest_int_dtype(
            pd.Series(sr_renamed.cat.categories.values),
        )
    elif "bool" in categories_type_str:
        categories_type_str = "bool[pyarrow]"
    else:
        categories_type_str = "str[pyarrow]"
    return _set_new_categories(
        sr_renamed,
        sr_renamed.cat.categories,
        ordered,
        categories_type_str,
    )


def _remove_delimiter_levels(sr, delimiter, nr_identifiers, ordered):
    categories_list = _categories_remove_nr_delimiter_levels(
        sr,
        delimiter,
        nr_identifiers,
    )
    categories_mapping = dict(zip(sr.cat.categories, categories_list, strict=False))
    return _renaming_categories(sr, categories_mapping, ordered)


def _set_new_categories(
    sr,
    categories,
    ordered,
    categories_type_str="str[pyarrow]",
) -> pd.Series:
    if categories.empty:
        return sr.astype("category")
    if "int" in categories_type_str:
        int_categories_dtype = find_lowest_int_dtype(categories)
        new_categorical = pd.CategoricalDtype(
            pd.Series(categories, dtype=int_categories_dtype),
            ordered=ordered,
        )
    else:
        new_categorical = pd.CategoricalDtype(
            pd.Series(categories, dtype=categories_type_str),
            ordered=ordered,
        )
    return pd.Series(sr.cat.rename_categories(categories), dtype=new_categorical)


def _categories_remove_nr_delimiter_levels(
    sr: pd.Series,
    delimiter: str,
    nr_identifiers: int,
) -> list:
    return [
        i.split(delimiter, nr_identifiers)[nr_identifiers]
        for i in sr.cat.categories
        if i.count(delimiter) >= nr_identifiers
    ]


def create_dummy(
    sr: pd.Series,
    true_value: bool | str | list,
    kind: str = "equality",
) -> pd.Series:
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


def agreement_to_int_categorical(
    sr: pd.Series,
    renaming: dict,
    ordered=True,
) -> pd.Series:  # type hint to pd.Series[pd.Category]
    """Clean the categories of a pd.Series of dtype category with unspecified type
    entries.

    Parameters:
        sr (pd.Series): The input series with categories to be cleaned.
        renaming (dict): A dictionary to rename the categories.
        ordered (bool, optional): Whether the series should be returned as unordered. Defaults to True.

    Returns:
        pd.Series[category]: The series with cleaned categories.

    """
    sr = str_categorical(
        sr,
        ordered=ordered,
        renaming=renaming,
        categories_type_str="int[pyarrow]",
    )
    return int_categorical(sr, ordered=ordered)


def bool_categorical(
    sr: pd.Series,
    renaming: dict | None = None,
    ordered: bool = False,
) -> pd.Series:
    """Clean the categories of a pd.Series of dtype category with str entries and change
    categories to bool.

    Parameters:
        sr (pd.Series): The input series to be cleaned.
        renaming (dict | None, optional): A dictionary to rename the categories. Defaults to None.
        ordered (bool, optional): Whether the series should be returned as ordered, order imputed from renaming keys. Defaults to False.

    Returns:
        pd.Series: The series with cleaned categories.

    """
    # TODO: discuss if this manipulation on the dataset is acceptable
    _error_handling_categorical(
        sr,
        "category",
        [
            [sr, "pandas.core.series.Series"],
            [renaming, "dict" if renaming is not None else "None"],
        ],
        [sr.cat.categories.to_list(), "str | int"],
    )
    sr = _remove_missing_data_categories(sr)
    if renaming is not None:
        sr = _renaming_categories(
            sr,
            renaming,
            ordered,
            categories_type_str="bool[pyarrow]",
        )
    return sr


def str_categorical(
    sr: pd.Series,
    nr_identifiers: int = 1,
    ordered: bool = False,
    renaming: dict | None = None,
    reduce: bool = False,
) -> pd.Series:
    """Clean and change the categories based on the renaming dict or remove identifier
    levels based on nr_identifiers of a pd.Series of dtype category with str entries.

    Parameters:
        sr (pd.Series[pd.CategoricalDtype[str]]): The input series with categories to be cleaned.
        nr_identifiers (int, optional): The number of identifiers inside each category to be removed. Defaults to 1.
        ordered (bool, optional): Whether the series should be returned as unordered. Defaults to True.
        renaming (dict | None, optional): A dictionary to rename the categories. Defaults to None.
        reduce (bool, optional): Whether the renaming mapping reduces the number of categories. Defaults to False.

    Returns:
        pd.Series[str]: The series with cleaned new categories.

    Example:
        >>> dtype = pd.CategoricalDtype(['[1] A XY', '[2] B BZ', '[3] D ZZ'], ordered=True)
        >>> sr = pd.Series(['[1] A XY', '[2] B BZ', '[3] D ZZ'], dtype=dtype)
        >>> str_categorical(sr, nr_identifiers=1)
        0    'A XY'
        1    'B BZ'
        2    'D ZZ'
        dtype: category
        Categories (3, object): ['A XY' < 'B BZ' < 'D ZZ']

        >>> str_categorical(sr, nr_identifiers=2, ordered=False)
        0    'XY'
        1    'BZ'
        2    'CZ'
        dtype: category
        Categories (3, object): ['XY', 'BZ', 'CZ']

        >>> str_categorical(sr, renaming={'[1] A XY': 'A', '[2] B BZ': 'B', '[3] D ZZ': 'C'})
        0    'A'
        1    'B'
        2    'C'
        dtype: category
        Categories (3, object): ['A' < 'B' < 'C']

    """
    _error_handling_categorical(
        sr,
        "category",
        [
            [sr, "pandas.core.series.Series"],
            [nr_identifiers, "int"],
            [ordered, "bool"],
            [renaming, "dict" if renaming is not None else "None"],
            [reduce, "bool"],
        ],
        [sr.cat.categories.to_list(), "str | int | float"],
    )

    sr_no_missing = _remove_missing_data_categories(sr)
    delimiter = " "

    if reduce:
        if renaming is None:
            renaming = dict(
                zip(
                    sr_no_missing.cat.categories.tolist(),
                    _categories_remove_nr_delimiter_levels(
                        sr_no_missing,
                        delimiter,
                        nr_identifiers,
                    ),
                    strict=False,
                ),
            )

        return _reduce_categories(sr_no_missing, renaming, ordered)
    if renaming is not None:
        return _renaming_categories(sr_no_missing, renaming, ordered)
    return _remove_delimiter_levels(
        sr_no_missing,
        delimiter,
        nr_identifiers,
        ordered,
    )


def str_categorical_to_int_categorical(
    sr: "pd.Series[category]",
    renaming: dict | None = None,
    ordered: bool = False,
    reduce: bool = False,
) -> "pd.Series[category]":
    """Transform a pd.Series of dtype category with str entries to dtype int entries.

    Parameters:
        sr (pd.Series[category]): The input series to be cleaned.
        renaming (dict | None, optional): A dictionary to rename the categories. Defaults to None.
        ordered (bool, optional): Whether the series should be returned as ordered. Defaults to False.

    Returns:
        pd.Series[category]: The series with cleaned categories.

    """
    _error_handling_categorical(
        sr,
        "category",
        [
            [sr, "pandas.core.series.Series"],
            [ordered, "bool"],
        ],
        [sr.cat.categories.to_list(), "str"],
    )
    sr_no_missing = _remove_missing_data_categories(sr)
    if renaming is not None:
        if reduce:
            return _reduce_categories(sr_no_missing, renaming, ordered)
        return _renaming_categories(
            sr_no_missing,
            renaming,
            ordered,
            "int[pyarrow]",
        )

    return int_to_int_categorical(
        apply_lowest_int_dtype(sr_no_missing),
        ordered,
    )


# TODO: inspect if replace should be changed to rename_categories


def int_categorical(
    sr: "pd.Series[int]",
    ordered: bool = False,
) -> "pd.Series[category]":
    """Clean the categories of a pd.Series of dtype category with int entries.

    Parameters:
        sr (pd.Series[int]): The input series to be cleaned.
        ordered (bool, optional): Whether the series should be returned as unordered. Defaults to False.

    Returns:
        pd.Series[int]: The series with cleaned categories.

    """
    _error_handling_categorical(
        sr,
        "category",
        [[sr, "pandas.core.series.Series"], [ordered, "bool"]],
        [sr.cat.categories.to_list(), "int"],
    )
    sr = _remove_missing_data_categories(sr)
    return int_to_int_categorical(apply_lowest_int_dtype(sr), ordered)


def int_categorical_to_int(sr: "pd.Series[category]") -> "pd.Series[int]":
    """Transform a pd.Series of dtype category with int entries to dtype int.

    Parameters:
        sr (pd.Series[category]): The input series to be cleaned.

    Returns:
        pd.Series[int]: The series with cleaned entries.

    """
    _error_handling_categorical(
        sr,
        "category",
        [[sr, "pandas.core.series.Series"]],
        [sr.cat.categories.to_list(), "int | str"],
    )
    sr = _remove_missing_data_categories(sr)
    return apply_lowest_int_dtype(sr)


def float_categorical_to_int(sr: pd.Series) -> pd.Series:
    """Transform a pd.Series of dtype category with float entries to dtype int.

    Parameters:
        sr (pd.Series): The input series to be cleaned.

    Returns:
        pd.Series: The series with cleaned entries.

    """
    _error_handling_categorical(
        sr,
        "category",
        [[sr, "pandas.core.series.Series"]],
        [sr.cat.categories.to_list(), "float | str"],
    )
    sr = _remove_missing_data_categories(sr)
    return apply_lowest_int_dtype(sr)


def int_to_int_categorical(
    sr: "pd.Series[int]",
    ordered: bool = False,
) -> "pd.Series[int]":
    """Transform a pd.Series with int entries to dtype (un)ordered category.

    Parameters:
        sr (pd.Series[int]): The input series to be cleaned.
        unordered (bool, optional): Whether the series should be returned as unordered. Defaults to False.

    Returns:
        pd.Series[int]: The series with int categories.

    """
    _fail_if_series_wrong_dtype(sr, "int")
    _fail_if_invalid_input(sr, "pandas.core.series.Series")
    _fail_if_invalid_input(ordered, "bool")
    sr[sr < 0] = pd.NA
    sr = sr.astype("category")
    return _set_new_categories(
        sr,
        sr.cat.categories,
        ordered,
        categories_type_str="int[pyarrow]",
    )


def categorical_to_int_categorical(
    sr: "pd.Series",
    renaming: dict,
    filter_renaming=False,
    ordered=True,
) -> "pd.Series[int]":
    """Clean the categories of a pd.Series of dtype category with unspecified type
    entries.

    Parameters:
        sr (pd.Series[str]): The input series with categories to be cleaned.
        renaming (dict): A dictionary to rename the categories.
        filer_renaming(bool, optional): Whether the renaming dictionary should by filtered against the current categories. Defaults to False
        ordered (bool, optional): Whether the series should be returned as unordered. Defaults to True.

    Returns:
        pd.Series[int]: The series with cleaned categories.

    """
    _error_handling_categorical(
        sr,
        "category",
        [[sr, "pandas.core.series.Series"], [renaming, "dict"]],
        [sr.cat.categories.to_list(), "int | str"],
    )
    sr_no_missing = _remove_missing_data_categories(sr)
    if filter_renaming:
        current_categories = sr_no_missing.cat.categories.to_list()
        renaming = {
            key: value
            for key, value in renaming.items()
            if any(category in key for category in current_categories)
        }
    """
    return _set_new_categories(
        sr_no_missing, renaming, ordered, categories_type_str="int[pyarrow]"
    )
    """
    return _renaming_categories(
        sr_no_missing,
        renaming,
        ordered,
        categories_type_str="int[pyarrow]",
    )


def float_categorical_to_float(sr: "pd.Series[category]") -> "pd.Series[float]":
    """Transform a pd.Series of dtype category with flota entries to dtype float.

    Parameters:
        sr (pd.Series[float]): The input series to be cleaned.

    Returns:
        pd.Series[float]: The series with cleaned categories.

    """
    _error_handling_categorical(
        sr,
        "category",
        [[sr, "pandas.core.series.Series"]],
        [sr.cat.categories.to_list(), "float | str"],
    )
    sr = _remove_missing_data_categories(sr)
    return apply_lowest_float_dtype(sr)


def replace_conditionally(
    sr: pd.Series,
    conditioning_sr: pd.Series,
    conditioning_value: str | int | float,
    replacement_value: str | int | float,
) -> pd.Series:
    """Replace values in a series based on a condition in another series.

    Parameters:
        sr (pd.Series): The series to be cleaned.
        conditioning_sr (pd.Series): The series that contains the condition.
        conditioning_value (str | int | float): The value in the conditioning series that triggers the replacement.
        replacement_value (str | int | float): The value that replaces the values in the series.

    Returns:
        pd.Series: The series with the replaced values.

    """
    # TODO: check this replacement style with Hans-Martin
    new_sr = sr.copy()
    new_sr.loc[conditioning_sr == conditioning_value] = replacement_value
    return new_sr


def find_lowest_int_dtype(sr: pd.Series) -> str:
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
        if sr.max() <= 255:
            return "uint8[pyarrow]"
        if sr.max() <= 65535:
            return "uint16[pyarrow]"
        if sr.max() <= 4294967295:
            return "uint32[pyarrow]"
        return "uint64[pyarrow]"
    if sr.min() >= -128 and sr.max() <= 127:
        return "int8[pyarrow]"
    if sr.min() >= -32768 and sr.max() <= 32767:
        return "int16[pyarrow]"
    if sr.min() >= -2147483648 and sr.max() <= 2147483647:
        return "int32[pyarrow]"
    return "int64[pyarrow]"


def apply_lowest_int_dtype(
    sr: pd.Series,
) -> pd.Series:
    """Apply the lowest integer dtype to a series."""
    try:
        return pd.to_numeric(sr, downcast="unsigned", dtype_backend="pyarrow")
    except ValueError:
        return pd.to_numeric(sr, downcast="integer", dtype_backend="pyarrow")


def apply_lowest_float_dtype(sr: pd.Series) -> pd.Series:
    """Apply the lowest integer dtype to a series."""
    return pd.to_numeric(sr, downcast="float", dtype_backend="pyarrow")
