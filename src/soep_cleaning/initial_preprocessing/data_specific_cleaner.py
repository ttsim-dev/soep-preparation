import pandas as pd

from soep_cleaning.utilities import find_lowest_int_dtype
from soep_cleaning.initial_preprocessing.data_cleaning_helper import categorical_string_cleaning, categorical_int_cleaning, categorical_bool_cleaning, transform_biobirth

def bioedu(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the bioedu dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = raw_data["cid"].astype(find_lowest_int_dtype(raw_data["cid"]))
    out["p_id"] = raw_data["pid"].astype(find_lowest_int_dtype(raw_data["pid"]))

    out["birth_month"] = categorical_string_cleaning(raw_data["gebmonat"], unordered=True)
    return out

def biobirth(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biobirth dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = raw_data["cid"].astype(find_lowest_int_dtype(raw_data["cid"]))
    out["p_id"] = raw_data["pid"].astype(find_lowest_int_dtype(raw_data["pid"]))

    out["n_kids_total"] = raw_data["sumkids"].astype(find_lowest_int_dtype(raw_data["sumkids"]))

    for i in range(1, 16):
        two_digit = f"{i:02d}"
        out[f"birth_year_child_{i}"] = categorical_int_cleaning(raw_data[f"kidgeb{two_digit}"])
        out[f"p_id_child_{i}"] = categorical_int_cleaning(raw_data[f"kidpnr{two_digit}"])
        out[f"birth_month_child_{i}"] = categorical_string_cleaning(raw_data[f"kidmon{two_digit}"], unordered=True)
        
    return transform_biobirth(out)

def biol(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["soep_hh_id"] = raw_data["hid"].astype(find_lowest_int_dtype(raw_data["hid"]))
    out["p_id"] = raw_data["pid"].astype(find_lowest_int_dtype(raw_data["pid"]))

    out["year"] = categorical_int_cleaning(raw_data["syear"])

    out["birthplace"] = categorical_string_cleaning(raw_data["lb0013_h"], unordered=True)
    out["res_childhood"] = categorical_string_cleaning(raw_data["lb0058"], unordered=True)
    out["birthplace_father"] = categorical_bool_cleaning(raw_data["lb0084_h"])
    out["birthplace_mother"] = categorical_bool_cleaning(raw_data["lb0085_h"])
    out["religion_father"] = categorical_string_cleaning(raw_data["lb0124_h"], unordered=True)
    out["religion_mother"] = categorical_string_cleaning(raw_data["lb0125_h"], unordered=True)

    return out

def design(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = raw_data["cid"].astype(find_lowest_int_dtype(raw_data["cid"]))
    out["hh_random_group"] = raw_data["rgroup"].astype(find_lowest_int_dtype(raw_data["rgroup"]))
    out["hh_strat"] = raw_data["strat"].astype(find_lowest_int_dtype(raw_data["strat"]))

    out["hh_soep_sample"] = categorical_string_cleaning(raw_data["hsample"], one_identifier_level=False, unordered=True)

    return out

def hgen(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = raw_data["cid"].astype(find_lowest_int_dtype(raw_data["cid"]))
    out["soep_hh_id"] = raw_data["hid"].astype(find_lowest_int_dtype(raw_data["hid"]))

    out["year"] = categorical_int_cleaning(raw_data["syear"])
    out["building_year_hh_max"] = categorical_int_cleaning(raw_data["hgcnstyrmax"])
    out["building_year_hh_min"] = categorical_int_cleaning(raw_data["hgcnstyrmin"])
    out["heating_costs_m_hh"] = categorical_int_cleaning(raw_data["hgheat"])
    out["einzugsjahr"] = categorical_int_cleaning(raw_data["hgmoveyr"])
    out["bruttokaltmiete_m_hh"] = categorical_int_cleaning(raw_data["hgrent"])
    out["living_space_hh"] = categorical_int_cleaning(raw_data["hgsize"])

    out["heizkosten_mi_reason"] = categorical_string_cleaning(raw_data["hgheatinfo"])
    out["rented_or_owned"] = categorical_string_cleaning(raw_data["hgowner"])
    out["hh_typ"] = categorical_string_cleaning(sr=raw_data["hgtyp1hh"], one_identifier_level=False)
    out["hh_typ_2st"] = categorical_string_cleaning(raw_data["hgtyp2hh"])
    
    out = out.melt(id_vars=["soep_initial_hh_id", "soep_hh_id"])
    return out.dropna(subset=['value']).reset_index(drop=True)

def hl(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["soep_hh_id"] = raw_data["hid"].astype(find_lowest_int_dtype(raw_data["hid"]))

    out["year"] = categorical_int_cleaning(raw_data["syear"])

    out["kindergeld_hl_m_hh_prev"] = categorical_int_cleaning(raw_data["hlc0042_h"])
    out["kindergeld_bezug_aktuell"] = categorical_bool_cleaning(raw_data["hlc0044_h"])
    out["kindergeld_aktuell_hl_m_hh"] = categorical_int_cleaning(raw_data["hlc0045_h"])
    out["kinderzuschlag_aktuell_hh"] = categorical_bool_cleaning(raw_data["hlc0046_h"])
    out["kinderzuschlag_hl_m_hh"] = categorical_int_cleaning(raw_data["hlc0047_h"])
    out["kinderzuschlag_hl_hh_prev"] = categorical_bool_cleaning(raw_data["hlc0049_h"])
    out["kinderzuschlag_hl_m_hh_prev"] = categorical_int_cleaning(raw_data["hlc0051_h"])
    out["alg2_months_soep_hh_prev"] = categorical_int_cleaning(raw_data["hlc0053"])
    out["arbeitsl_geld_2_soep_m_hh_prev"] = categorical_int_cleaning(raw_data["hlc0054"])
    out["alg2_etc_aktuell_hh"] = categorical_bool_cleaning(raw_data["hlc0064_h"])
    out["hilfe_lebensunterh_aktuell_hh"] = categorical_bool_cleaning(raw_data["hlc0067_h"])
    out["wohngeld_soep_m_hh_prev"] = categorical_int_cleaning(raw_data["hlc0082_h"])
    out["wohngeld_aktuell_hh"] = categorical_bool_cleaning(raw_data["hlc0083_h"])
    out["betreu_kosten_pro_kind"] = categorical_int_cleaning(raw_data["hld0009"])

    out = out.melt(id_vars=["soep_hh_id"])
    return out.dropna(subset=['value']).reset_index(drop=True)