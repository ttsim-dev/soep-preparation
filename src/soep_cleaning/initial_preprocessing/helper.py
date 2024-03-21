from collections import OrderedDict

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


def _fail_if_invalid_input(inputt, expected_dtype: str):
    if expected_dtype not in str(type(inputt)):
        msg = f"Expected {inputt} to be of type {expected_dtype}, got {type(inputt)}"
        raise TypeError(
            msg,
        )


def _error_handling_categorical(
    sr: pd.Series,
    expected_sr_dtype: str,
    input_expected_types: list[list] | None = None,
):
    if input_expected_types is None:
        input_expected_types = [[]]
    _fail_if_series_wrong_dtype(sr, expected_sr_dtype)
    _fail_series_without_categories(sr)
    for item in input_expected_types:
        inputt, expected_type = item
        _fail_if_invalid_input(inputt, expected_type)


def _remove_missing_data_categories(sr: pd.Series) -> pd.Series:
    """Remove categories representing missing data or no response to the questionnaire
    from a categorical Series.

    Parameters:
        sr (pd.Series): A pandas Series of dtype 'category' containing the categorical data.

    Returns:
        pd.Series: A new categorical Series with the missing data categories removed.

    """
    if -8 in sr.cat.categories:
        sr = sr.cat.remove_categories(-8)
    str_categories = sr.cat.categories[
        sr.cat.categories.map(lambda x: isinstance(x, str))
    ]
    removing_categories = [
        i for i in str_categories if i.startswith("[-") and i[3] == "]"
    ]
    return sr.cat.remove_categories(removing_categories)


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
    )
    sr = sr.astype("category")
    sr = _remove_missing_data_categories(sr)
    if renaming is not None:
        sr = sr.cat.rename_categories(renaming)
    if ordered:
        return sr.cat.reorder_categories(list(renaming.values()), ordered=True)
    else:
        return sr.cat.as_unordered()


def str_categorical(
    sr: "pd.Series[str]",
    no_identifiers: int = 1,
    ordered: bool = True,
    renaming: dict | None = None,
) -> "pd.Series[str]":
    """Clean (and potential rename and reduce number of) the categories of a pd.Series
    of dtype category with str entries.

    Parameters:
        sr (pd.Series[str]): The input series with categories to be cleaned.
        no_identifiers (int, optional): The number of identifiers inside each category to be removed. Defaults to 1.
        ordered (bool, optional): Whether the series should be returned as unordered. Defaults to True.
        renaming (dict | None, optional): A dictionary to rename the categories. Defaults to None.

    Returns:
        pd.Series[str]: The series with cleaned categories.

    Example:
        >>> sr = pd.Series(['[1] A 1984 Ausgangs-Sample (West)', '[2] B 1984 Migration (bis 1983, West)', '[3] C 1990 Ausgangs-Sample (Ost)'])
        >>> str_categorical(sr, no_identifiers=1)
        0    'A 1984 Ausgangs-Sample (West)'
        1    'B 1984 Migration (bis 1983, West)'
        2    'C 1990 Ausgangs-Sample (Ost)'
        dtype: category
        Categories (3, object): ['A 1984 Ausgangs-Sample (West)', 'B 1984 Migration (bis 1983, West)', 'C 1990 Ausgangs-Sample (Ost)']

        >>> str_categorical(sr, no_identifiers=2)
        0    '1984 Ausgangs-Sample (West)'
        1    '1984 Migration (bis 1983, West)'
        2    '1990 Ausgangs-Sample (Ost)'
        dtype: category
        Categories (3, object): ['1984 Ausgangs-Sample (West)', '1984 Migration (bis 1983, West)', '1990 Ausgangs-Sample (Ost)']

        >>> str_categorical(sr, renaming={'[1] A 1984 Ausgangs-Sample (West)': 'A', '[2] B 1984 Migration (bis 1983, West)': 'B', '[3] C 1990 Ausgangs-Sample (Ost)': 'C'})
        0    'A'
        1    'B'
        2    'C'
        dtype: category
        Categories (3, object): ['A', 'B', 'C']

    """
    _error_handling_categorical(
        sr,
        "category",
        [[sr, "pandas.core.series.Series"], [no_identifiers, "int"], [ordered, "bool"]],
    )
    sr = _remove_missing_data_categories(sr)
    if not ordered:
        sr = sr.cat.as_unordered()
    if renaming is not None:
        new_categories = list(OrderedDict.fromkeys(renaming.values()))
        sr = (
            sr.astype("str")
            .replace(renaming)
            .astype("category")
            .cat.remove_categories("nan")
            .cat.reorder_categories(new_categories, ordered=ordered)
        )
    else:
        # TODO: Check if the following lines are adequate
        sr = sr.cat.rename_categories(
            [i.split(" ", no_identifiers)[no_identifiers] for i in sr.cat.categories],
        )
    return sr


def int_categorical(sr: "pd.Series[int]", ordered: bool = False) -> "pd.Series[int]":
    """Clean the categories of a pd.Series of dtype category with int entries.

    Parameters:
        sr (pd.Series[int]): The input series to be cleaned.
        unordered (bool, optional): Whether the series should be returned as unordered. Defaults to False.

    Returns:
        pd.Series[int]: The series with cleaned categories.

    """
    _error_handling_categorical(
        sr,
        "category",
        [[sr, "pandas.core.series.Series"], [ordered, "bool"]],
    )
    sr = _remove_missing_data_categories(sr)
    return int_to_int_categorical(sr, ordered)


def int_categorical_to_int(sr: "pd.Series[category]") -> "pd.Series[int]":
    """Transform a pd.Series of dtype category with int entries to dtype int.

    Parameters:
        sr (pd.Series[int]): The input series to be cleaned.

    Returns:
        pd.Series[int]: The series with cleaned categories.

    """
    _error_handling_categorical(sr, "category", [[sr, "pandas.core.series.Series"]])
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
    sr = str_categorical(
        sr,
        ordered=ordered,
        renaming=renaming,
    )
    return int_categorical(sr, ordered=ordered)


def float_categorical_to_float(sr: "pd.Series[category]") -> "pd.Series[float]":
    """Transform a pd.Series of dtype category with flota entries to dtype float.

    Parameters:
        sr (pd.Series[float]): The input series to be cleaned.

    Returns:
        pd.Series[float]: The series with cleaned categories.

    """
    _error_handling_categorical(sr, "category", [[sr, "pandas.core.series.Series"]])
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
