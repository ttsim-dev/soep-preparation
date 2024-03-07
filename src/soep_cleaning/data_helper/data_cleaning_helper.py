import pandas as pd

from soep_cleaning.config import CATEGORIES_TO_REMOVE
from soep_cleaning.utilities import find_lowest_int_dtype

def _remove_irrelevant_categories(sr: pd.Series) -> pd.Series:
    """Remove irrelevant categories from a series."""
    str_categories = sr.cat.categories[sr.cat.categories.map(lambda x: isinstance(x, str))]
    removing_categories = list(set(str_categories) & CATEGORIES_TO_REMOVE)
    return sr.cat.remove_categories(removing_categories)

def _categorical_string_cleaning(sr: pd.Series, one_identifier_level: bool=True) -> pd.Series:
    """Clean categorial categories with preceiding numbering."""
    sr = _remove_irrelevant_categories(sr)
    if one_identifier_level:
        return sr.cat.rename_categories([i.split(" ", 1)[1] for i in sr.cat.categories])
    else:
        return sr.cat.rename_categories([i.split(" ", 2)[2] for i in sr.cat.categories])

def _categorical_int_cleaning(sr: pd.Series) -> pd.Series:
    """Transform a series to an ordered categorical containing integers."""
    if sr.dtype.name == "category":
        sr = _remove_irrelevant_categories(sr)
    sr = sr.astype(find_lowest_int_dtype(sr))
    categories_order = sr.sort_values().unique().dropna().tolist()
    sr = sr.astype("category")
    return sr.cat.set_categories(categories_order, rename=True, ordered=True)