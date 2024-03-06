import pandas as pd
import re

from data_management_von_gaudecker.data_helper.data_cleaning_helper import _integer_category_cleaning, _categorical_string_cleaning

def bioedu(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the bioedu dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = raw_data["cid"].astype("Int32")
    out["p_id"] = raw_data["pid"].astype("Int32")

    out["birth_month"] = _categorical_string_cleaning(raw_data["gebmonat"])
    out = out.melt(id_vars=["soep_initial_hh_id", "p_id"])
    return out.dropna(subset=['value']).reset_index(drop=True)

def biobirth(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biobirth dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = raw_data["cid"].astype("Int32")
    out["p_id"] = raw_data["pid"].astype("Int32")

    out["n_kids_total"] = raw_data["sumkids"].astype("Int16")

    for i in range(1, 16):
        two_digit = f"{i:02d}"
        out[f"birth_year_child_{i}"] = _integer_category_cleaning(raw_data[f"kidgeb{two_digit}"])
        out[f"p_id_child_{i}"] = _integer_category_cleaning(raw_data[f"kidpnr{two_digit}"])
        out[f"birth_month_child_{i}"] = _categorical_string_cleaning(raw_data[f"kidmon{two_digit}"])

    out = out.melt(id_vars=["soep_initial_hh_id", "p_id"])
    return out.dropna(subset=['value']).reset_index(drop=True)

def biol(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["soep_hh_id"] = raw_data["hid"].astype("Int32")
    out["p_id"] = raw_data["pid"].astype("Int32")

    out["year"] = raw_data["syear"].astype("Int16")

    out["birthplace"] = _categorical_string_cleaning(raw_data["lb0013_h"])
    out["res_childhood"] = _categorical_string_cleaning(raw_data["lb0058"])
    out["birthplace_father"] = _categorical_string_cleaning(raw_data["lb0084_h"])
    out["birthplace_mother"] = _categorical_string_cleaning(raw_data["lb0085_h"])
    out["religion_father"] = _categorical_string_cleaning(raw_data["lb0124_h"])
    out["religion_mother"] = _categorical_string_cleaning(raw_data["lb0125_h"])

    out = out.melt(id_vars=["soep_hh_id", "p_id"])
    return out.dropna(subset=['value']).reset_index(drop=True)