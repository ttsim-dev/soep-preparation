import pandas as pd

from data_management_von_gaudecker.config import CATEGORIES_TO_REMOVE

def _remove_irrelevant_categories(sr: pd.Series) -> pd.Series:
    """Remove irrelevant categories from a series."""
    removing_categories = list(set(sr.cat.categories) & set(CATEGORIES_TO_REMOVE))
    return sr.cat.remove_categories(removing_categories)

def _integer_category_cleaning(sr: pd.Series) -> pd.Series:
    """Transform a series to an integer."""
    sr = _remove_irrelevant_categories(sr)
    return sr.cat.rename_categories(sr.cat.categories.astype("Int32"))

def _categorical_string_cleaning(sr: pd.Series) -> pd.Series:
    """Set the categories of the birth month of a child."""
    sr = _remove_irrelevant_categories(sr)
    return sr.cat.rename_categories([i.split(" ", 1)[1] for i in sr.cat.categories])