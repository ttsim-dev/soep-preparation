import pandas as pd

from soep_cleaning.data_helper.data_cleaning_helper import _categorical_string_cleaning, _categorical_int_cleaning

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
        out[f"birth_year_child_{i}"] = _categorical_int_cleaning(raw_data[f"kidgeb{two_digit}"])
        out[f"p_id_child_{i}"] = _categorical_int_cleaning(raw_data[f"kidpnr{two_digit}"])
        out[f"birth_month_child_{i}"] = _categorical_string_cleaning(raw_data[f"kidmon{two_digit}"])

    out = out.melt(id_vars=["soep_initial_hh_id", "p_id"])
    return out.dropna(subset=['value']).reset_index(drop=True)

def biol(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["soep_hh_id"] = raw_data["hid"].astype("Int32")
    out["p_id"] = raw_data["pid"].astype("Int32")

    out["year"] = _categorical_int_cleaning(raw_data["syear"])

    out["birthplace"] = _categorical_string_cleaning(raw_data["lb0013_h"])
    out["res_childhood"] = _categorical_string_cleaning(raw_data["lb0058"])
    out["birthplace_father"] = _categorical_string_cleaning(raw_data["lb0084_h"])
    out["birthplace_mother"] = _categorical_string_cleaning(raw_data["lb0085_h"])
    out["religion_father"] = _categorical_string_cleaning(raw_data["lb0124_h"])
    out["religion_mother"] = _categorical_string_cleaning(raw_data["lb0125_h"])

    out = out.melt(id_vars=["soep_hh_id", "p_id"])
    return out.dropna(subset=['value']).reset_index(drop=True)

def design(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = raw_data["cid"].astype("Int32")
    out["hh_random_group"] = raw_data["rgroup"].astype("Int8")
    out["hh_strat"] = raw_data["strat"].astype("Int16")

    out["hh_random_group"] = _categorical_string_cleaning(raw_data["hsample"])

    out = out.melt(id_vars=["soep_initial_hh_id"])
    return out.dropna(subset=['value']).reset_index(drop=True)

def hgen(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = raw_data["cid"].astype("Int32")
    out["soep_hh_id"] = raw_data["hid"].astype("Int32")
    out["year"] = _categorical_int_cleaning(raw_data["syear"])
    out["building_year_hh_max"] = _categorical_int_cleaning(raw_data["hgcnstyrmax"])
    out["building_year_hh_min"] = _categorical_int_cleaning(raw_data["hgcnstyrmin"])
    out["heating_costs_m_hh"] = _categorical_int_cleaning(raw_data["hgheat"])
    out["heizkosten_mi_reason"] = _categorical_string_cleaning(raw_data["hgheatinfo"])
    out["einzugsjahr"] = _categorical_int_cleaning(raw_data["hgmoveyr"])
    out["rented_or_owned"] = _categorical_string_cleaning(raw_data["hgowner"])
    out["bruttokaltmiete_m_hh"] = _categorical_int_cleaning(raw_data["hgrent"])
    out["living_space_hh"] = _categorical_int_cleaning(raw_data["hgsize"])
    out["hh_typ"] = _categorical_string_cleaning(sr=raw_data["hgtyp1hh"], one_identifier_level=False)
    out["hh_typ_2st"] = _categorical_string_cleaning(raw_data["hgtyp2hh"])
    
    out = out.melt(id_vars=["soep_initial_hh_id", "soep_hh_id"])
    return out.dropna(subset=['value']).reset_index(drop=True)