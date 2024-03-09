import pandas as pd

from soep_cleaning.utilities import find_lowest_int_dtype


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
    return sr.cat.remove_categories(removing_categories)


def str_categorical(
    sr: "pd.Series[str]",
    no_identifiers: int = 1,
    unordered: bool = False,
) -> "pd.Series[str]":
    """Clean the categories of a pd.Series of dtype category with str entries.

    Parameters:
        sr (pd.Series[str]): The input series with categories to be cleaned.
        no_identifiers (int, optional): The number of identifiers inside each category to be removed. Defaults to 1.
        unordered (bool, optional): Whether the series should be returned as unordered. Defaults to False.

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

    """
    sr = _remove_missing_data_categories(sr)
    if unordered:
        sr = sr.cat.as_unordered()
    return sr.cat.rename_categories(
        [i.split(" ", no_identifiers)[no_identifiers] for i in sr.cat.categories],
    )


def int_categorical(sr: "pd.Series[int]", ordered: bool = False) -> "pd.Series[int]":
    """Clean the categories of a pd.Series of dtype category with int entries.

    Parameters:
        sr (pd.Series[int]): The input series to be cleaned.
        unordered (bool, optional): Whether the series should be returned as unordered. Defaults to False.

    Returns:
        pd.Series[int]: The series with cleaned categories.

    """
    sr = _remove_missing_data_categories(sr)
    return int_to_int_categorical(sr, ordered)


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
    sr = sr.astype(find_lowest_int_dtype(sr))
    categories_order = sr.sort_values().unique().dropna().tolist()
    sr = sr.astype("category")
    return sr.cat.set_categories(categories_order, rename=True, ordered=ordered)


def bool_categorical(sr: "pd.Series[str]") -> "pd.Series[bool]":
    """Clean the categories of a pd.Series of dtype category with str entries and change
    categories to bool.

    Parameters:
        sr (pd.Series[int]): The input series to be cleaned.
        unordered (bool, optional): Whether the series should be returned as unordered. Defaults to False.

    Returns:
        pd.Series[int]: The series with cleaned categories.

    """
    sr = sr.astype("category")
    return sr.cat.set_categories([False, True], rename=True, ordered=True)


def biobirth_wide_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """Transform the biobirth dataset from wide to long format.

    Parameters:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: The dataframe in long format.

    """
    prev_wide_cols = ["birth_year_child", "p_id_child", "birth_month_child"]
    df = pd.wide_to_long(
        df,
        stubnames=prev_wide_cols,
        i=["soep_initial_hh_id", "p_id", "n_kids_total"],
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


def int_categorical_to_int(sr: "pd.Series[category]") -> "pd.Series[int]":
    """Transform a pd.Series of dtype category with int entries to dtype int.

    Parameters:
        sr (pd.Series[int]): The input series to be cleaned.

    Returns:
        pd.Series[int]: The series with cleaned categories.

    """
    sr = _remove_missing_data_categories(sr)
    return sr.astype(find_lowest_int_dtype(sr))
