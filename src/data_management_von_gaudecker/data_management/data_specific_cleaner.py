import pandas as pd

from data_management_von_gaudecker.data_helper.data_cleaning_helper import _integer_category_cleaning, _month_category_cleaning

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
    """Melt the biobirth dataset from wide to long format."""
    out = data.melt(id_vars=["hh_id_orig", "p_id", "n_kids_total"])
    return out.dropna(subset=['value']).reset_index(drop=True)
