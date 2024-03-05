import pandas as pd
from pathlib import Path

from data_management_von_gaudecker.config import SRC, BLD

CATEGORIES_TO_REMOVE = ["[-5] In Fragebogenversion nicht enthalten", "[-3] nicht valide", "[-2] trifft nicht zu", "[-1] keine Angabe"] # move to different place


def _remove_irrelevant_categories(sr: pd.Series) -> pd.Series:
    """Remove irrelevant categories from a series."""
    removing_categories = list(set(sr.cat.categories) & set(CATEGORIES_TO_REMOVE))
    return sr.cat.remove_categories(removing_categories)

def _integer_category_cleaning(sr: pd.Series) -> pd.Series:
    """Transform a series to an integer."""
    sr = _remove_irrelevant_categories(sr)
    return sr.cat.rename_categories(sr.cat.categories.astype("Int64"))

def _month_category_cleaning(sr: pd.Series) -> pd.Series:
    """Set the categories of the birth month of a child."""
    sr = _remove_irrelevant_categories(sr)
    return sr.cat.rename_categories([i.split(" ")[1] for i in sr.cat.categories])


def clean_biobirth(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biobirth dataset."""
    out = pd.DataFrame()
    out["hh_id_orig"] = raw_data["cid"].astype("Int64")
    out["p_id"] = raw_data["pid"].astype("Int64")
    out["n_kids_total"] = raw_data["sumkids"].astype("Int64")
    for i in range(1, 16):
        two_digit = f"{i:02d}"
        out[f"birth_year_child_{i}"] = _integer_category_cleaning(raw_data[f"kidgeb{two_digit}"])
        out[f"p_id_child_{i}"] = _integer_category_cleaning(raw_data[f"kidpnr{two_digit}"])
        out[f"birth_month_child_{i}"] = _month_category_cleaning(raw_data[f"kidmon{two_digit}"])
    return out

def melt_biobirth(data: pd.DataFrame) -> pd.DataFrame:
    """Melt the biobirth dataset."""
    out = data.melt(id_vars=["hh_id_orig", "p_id", "n_kids_total"])
    return out.dropna(subset=['value']).reset_index(drop=True)
