import pandas as pd

from soep_cleaning.utilities import (
    apply_lowest_float_dtype,
    apply_lowest_int_dtype,
    find_lowest_float_dtype,
    find_lowest_int_dtype,
)


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
    if -8 in sr.cat.categories:
        removing_categories.append(-8)
    return sr.cat.set_categories(sr.cat.categories.drop(removing_categories))


def _renaming_categories(sr, renaming, ordered, integers):
    """Rename the categories of a pd.Series of dtype category based on the renaming
    dict.

    Parameters:
        sr (pd.Series): A pandas Series of dtype 'category' containing the categorical data.
        renaming (dict): A dictionary to rename the categories.
        ordered (bool): Whether the series should be returned as ordered, order imputed from renaming keys.
        integers (bool,optional): Whether the renaming keys are integers. Defaults to False.

    Return:
        pd.Series: A new categorical Series with the renamed categories.

    """
    sr_renamed = sr.cat.reorder_categories(
        renaming,
        ordered=ordered,
    ).cat.rename_categories(renaming)
    categories_type_str = (
        find_lowest_int_dtype(sr_renamed.cat.categories.values)
        if integers
        else "str[pyarrow]"
    )
    category_dtype = pd.CategoricalDtype(
        sr_renamed.cat.categories.astype(categories_type_str),
        ordered,
    )
    return pd.Series(sr_renamed, dtype=category_dtype)


def _remove_delimiter_levels(sr, delimiter, nr_identifiers, ordered):
    categories_list = _categories_remove_nr_delimiter_levels(
        sr,
        delimiter,
        nr_identifiers,
    )
    categories = (
        pd.Categorical(categories_list).astype("str[pyarrow]").astype("category")
    )
    sr_new_categories = sr.cat.set_categories(categories)
    if ordered:
        return sr_new_categories.cat.as_ordered()
    else:
        return sr_new_categories


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


def bool_categorical(
    sr: "pd.Series[str]",
    renaming: dict | None = None,
    ordered: bool = False,
) -> "pd.Series[bool]":
    """Clean the categories of a pd.Series of dtype category with str entries and change
    categories to bool.

    Parameters:
        sr (pd.Series[int]): The input series to be cleaned.
        renaming (dict | None, optional): A dictionary to rename the categories. Defaults to None.
        ordered (bool, optional): Whether the series should be returned as ordered, order imputed from renaming keys. Defaults to False.

    Returns:
        pd.Series[int]: The series with cleaned categories.

    """
    _error_handling_categorical(
        sr,
        "category",
        [
            [sr, "pandas.core.series.Series"],
            [renaming, "dict" if renaming is not None else "None"],
        ],
        [sr.cat.categories.to_list(), "str"],
    )
    sr = sr.astype("category")  # TODO: check necessity, might be redundant
    sr = _remove_missing_data_categories(sr)
    if renaming is not None:
        sr = sr.cat.rename_categories(renaming)
    if ordered:
        return sr.cat.reorder_categories(list(renaming.values()), ordered=True)
    else:
        return sr.cat.as_unordered()


def str_categorical(
    sr: "pd.Series[pd.CategoricalDtype[str]]",
    nr_identifiers: int = 1,
    ordered: bool = True,
    renaming: dict | None = None,
) -> "pd.Series[pd.CategoricalDtype[str]]":
    """Clean and change the categories based on the renaming dict or remove identifier
    levels based on nr_identifiers of a pd.Series of dtype category with str entries.

    Parameters:
        sr (pd.Series[pd.CategoricalDtype[str]]): The input series with categories to be cleaned.
        nr_identifiers (int, optional): The number of identifiers inside each category to be removed. Defaults to 1.
        ordered (bool, optional): Whether the series should be returned as unordered. Defaults to True.
        renaming (dict | None, optional): A dictionary to rename the categories. Defaults to None.

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
        ],
        [sr.cat.categories.to_list(), "str"],
    )

    sr_no_missing = _remove_missing_data_categories(sr)
    delimiter = " "

    if renaming is not None:
        return _renaming_categories(sr_no_missing, renaming, ordered)
    else:
        return _remove_delimiter_levels(
            sr_no_missing,
            delimiter,
            nr_identifiers,
            ordered,
        )


def int_categorical(sr: "pd.Series[int]", ordered: bool = False) -> "pd.Series[int]":
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
        pd.Series[int]: The series with cleaned categories.

    """
    _error_handling_categorical(
        sr,
        "category",
        [[sr, "pandas.core.series.Series"]],
        [sr.cat.categories.to_list(), "int | str"],
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
    sr = apply_lowest_int_dtype(sr)
    categories_order = sr.sort_values().unique().dropna().tolist()
    sr = sr.astype("category")
    return sr.cat.set_categories(categories_order, rename=True, ordered=ordered)


def agreement_int_categorical(
    sr: "pd.Series",
    renaming: dict,
    ordered=True,
) -> "pd.Series[int]":
    """Clean the categories of a pd.Series of dtype category with unspecified type
    entries.

    Parameters:
        sr (pd.Series[str]): The input series with categories to be cleaned.
        renaming (dict): A dictionary to rename the categories.
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
    return _renaming_categories(sr_no_missing, renaming, ordered, integers=True)


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


def biobirth_wide_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """Transform the biobirth dataset from wide to long format.

    Parameters:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: The dataframe in long format.

    """
    _fail_if_invalid_input(df, "pandas.core.frame.DataFrame")
    prev_wide_cols = ["birth_year_child", "p_id_child", "birth_month_child"]
    df = pd.wide_to_long(
        df,
        stubnames=prev_wide_cols,
        i=["soep_initial_hh_id", "p_id"],
        j="child_number",
        sep="_",
    ).reset_index()
    df = df.dropna(subset=prev_wide_cols, how="all")
    return df.astype(
        {
            "birth_year_child": find_lowest_int_dtype(df["birth_year_child"]),
            "p_id_child": find_lowest_int_dtype(df["p_id_child"]),
            "birth_month_child": "category",
        },
    )


def hwealth_wide_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """Transform the hwealth dataset from wide to long format.

    Parameters:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: The dataframe in long format.

    """
    _fail_if_invalid_input(df, "pandas.core.frame.DataFrame")
    prev_wide_cols = [
        "wohnsitz_immobilienverm_hh",
        "finanzverm_hh",
        "bruttoverm_hh",
        "nettoverm_hh",
        "wert_fahrzeuge",
        "bruttoverm_inkl_fahrz_hh",
        "nettoverm_fahrz_kredit_hh",
    ]
    df = pd.wide_to_long(
        df,
        stubnames=prev_wide_cols,
        i=["year", "soep_hh_id"],
        j="var",
        sep="_",
        suffix=r"\w+",
    ).reset_index()
    df = df.dropna(subset=prev_wide_cols, how="all")
    return df.astype(
        {
            "wohnsitz_immobilienverm_hh": find_lowest_float_dtype(
                df["wohnsitz_immobilienverm_hh"],
            ),
            "finanzverm_hh": find_lowest_float_dtype(df["finanzverm_hh"]),
            "bruttoverm_hh": find_lowest_float_dtype(df["bruttoverm_hh"]),
            "nettoverm_hh": find_lowest_float_dtype(df["nettoverm_hh"]),
            "wert_fahrzeuge": find_lowest_int_dtype(df["wert_fahrzeuge"]),
            "bruttoverm_inkl_fahrz_hh": find_lowest_float_dtype(
                df["bruttoverm_inkl_fahrz_hh"],
            ),
            "nettoverm_fahrz_kredit_hh": find_lowest_float_dtype(
                df["nettoverm_fahrz_kredit_hh"],
            ),
        },
    )
