import pandas as pd

from soep_cleaning.utilities import find_lowest_int_dtype


def _remove_irrelevant_categories(sr: pd.Series) -> pd.Series:
    """Remove irrelevant categories from a series."""
    str_categories = sr.cat.categories[
        sr.cat.categories.map(lambda x: isinstance(x, str))
    ]
    removing_categories = [
        i for i in str_categories if i.startswith("[-") and i[3] == "]"
    ]
    return sr.cat.remove_categories(removing_categories)


def categorical_string_cleaning(
    sr: pd.Series,
    one_identifier_level: bool = True,
    unordered: bool = False,
) -> pd.Series:
    """Clean categorial categories with preceiding numbering.

    example of one_identifier_level in docstring

    """
    sr = _remove_irrelevant_categories(sr)
    if unordered:
        sr = sr.cat.as_unordered()
    if one_identifier_level:
        return sr.cat.rename_categories([i.split(" ", 1)[1] for i in sr.cat.categories])
    else:
        return sr.cat.rename_categories([i.split(" ", 2)[2] for i in sr.cat.categories])


def categorical_int_cleaning(sr: pd.Series, ordered: bool = True) -> pd.Series:
    """Transform a series to an ordered categorical containing integers."""
    if sr.dtype.name == "category":
        sr = _remove_irrelevant_categories(sr)
    sr = sr.astype(find_lowest_int_dtype(sr))
    categories_order = sr.sort_values().unique().dropna().tolist()
    sr = sr.astype("category")
    return sr.cat.set_categories(categories_order, rename=True, ordered=ordered)


def categorical_bool_cleaning(sr: pd.Series) -> pd.Series:
    """Transform a series to an ordered categorical containing booleans."""
    sr = sr.astype("category")
    return sr.cat.set_categories([False, True], rename=True, ordered=True)


def biobirth_wide_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """Transform the biobirth dataset from wide to long format."""
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


def categorical_to_int_cleaning(sr: pd.Series) -> pd.Series:
    """Remove irrelevant categories and transform to integer."""
    if sr.dtype.name == "category":
        sr = _remove_irrelevant_categories(sr)
    return sr.astype(find_lowest_int_dtype(sr))
