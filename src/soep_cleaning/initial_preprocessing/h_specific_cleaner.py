import pandas as pd

from soep_cleaning.initial_preprocessing.helper import (
    bool_categorical,
    hwealth_wide_to_long,
    int_categorical_to_int,
    str_categorical,
)
from soep_cleaning.utilities import apply_lowest_float_dtype, apply_lowest_int_dtype


def hgen(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = apply_lowest_int_dtype(raw_data["cid"])
    out["soep_hh_id"] = apply_lowest_int_dtype(raw_data["hid"])

    out["year"] = apply_lowest_int_dtype(raw_data["syear"])
    out["building_year_hh_max"] = int_categorical_to_int(raw_data["hgcnstyrmax"])
    out["building_year_hh_min"] = int_categorical_to_int(raw_data["hgcnstyrmin"])
    out["heating_costs_m_hh"] = int_categorical_to_int(raw_data["hgheat"])
    out["einzugsjahr"] = int_categorical_to_int(raw_data["hgmoveyr"])
    out["bruttokaltmiete_m_hh"] = int_categorical_to_int(raw_data["hgrent"])
    out["living_space_hh"] = int_categorical_to_int(raw_data["hgsize"])

    out["heizkosten_mi_reason"] = str_categorical(
        raw_data["hgheatinfo"],
        ordered=False,
    )
    out["rented_or_owned"] = str_categorical(
        raw_data["hgowner"],
        ordered=False,
    )
    out["hh_typ"] = str_categorical(
        raw_data["hgtyp1hh"],
        no_identifiers=2,
        ordered=False,
    )
    out["hh_typ_2st"] = str_categorical(
        raw_data["hgtyp2hh"],
        ordered=False,
    )

    return out


def hl(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["soep_hh_id"] = int_categorical_to_int(raw_data["hid"])
    out["year"] = int_categorical_to_int(raw_data["syear"])

    out["kindergeld_hl_m_hh_prev"] = int_categorical_to_int(raw_data["hlc0042_h"])
    out["kindergeld_aktuell_hl_m_hh"] = int_categorical_to_int(raw_data["hlc0045_h"])
    out["kinderzuschlag_hl_m_hh"] = int_categorical_to_int(raw_data["hlc0047_h"])
    out["kinderzuschlag_hl_m_hh_prev"] = int_categorical_to_int(raw_data["hlc0051_h"])
    out["alg2_months_soep_hh_prev"] = int_categorical_to_int(raw_data["hlc0053"])
    out["arbeitsl_geld_2_soep_m_hh_prev"] = int_categorical_to_int(raw_data["hlc0054"])

    out["betreu_kosten_pro_kind"] = bool_categorical(raw_data["hlc0009"])
    out["kindergeld_bezug_aktuell"] = bool_categorical(raw_data["hlc0044_h"])
    out["kinderzuschlag_aktuell_hh"] = bool_categorical(raw_data["hlc0046_h"])
    out["kinderzuschlag_hl_hh_prev"] = bool_categorical(raw_data["hlc0049_h"])
    out["alg2_etc_aktuell_hh"] = bool_categorical(raw_data["hlc0064_h"])
    out["hilfe_lebensunterh_aktuell_hh"] = bool_categorical(
        raw_data["hlc0067_h"],
    )
    out["wohngeld_soep_m_hh_prev"] = int_categorical_to_int(raw_data["hlc0082_h"])
    out["wohngeld_aktuell_hh"] = bool_categorical(raw_data["hlc0083_h"])

    return out


def hpathl(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the hpathl dataset."""
    out = pd.DataFrame()
    out["soep_hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["year"] = apply_lowest_int_dtype(raw_data["syear"])
    out["hh_soep_sample"] = str_categorical(
        raw_data["hsample"],
        no_identifiers=2,
        ordered=False,
    )

    out["hh_bleibe_wkeit"] = apply_lowest_float_dtype(raw_data["hbleib"])
    out["hh_gewicht"] = apply_lowest_float_dtype(raw_data["hhrf"])
    out["hh_gewicht_nur_neue"] = apply_lowest_float_dtype(raw_data["hhrf0"])
    out["hh_gewicht_ohne_neue"] = apply_lowest_float_dtype(raw_data["hhrf0"])
    return out


def hwealth(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the hwealth dataset."""
    out = pd.DataFrame()
    out["year"] = apply_lowest_int_dtype(raw_data["syear"])
    out["soep_hh_id"] = apply_lowest_int_dtype(raw_data["hid"])

    out["wohnsitz_immobilienverm_hh_a"] = apply_lowest_float_dtype(raw_data["p010ha"])
    out["wohnsitz_immobilienverm_hh_b"] = apply_lowest_float_dtype(raw_data["p010hb"])
    out["wohnsitz_immobilienverm_hh_c"] = apply_lowest_float_dtype(raw_data["p010hc"])
    out["wohnsitz_immobilienverm_hh_d"] = apply_lowest_float_dtype(raw_data["p010hd"])
    out["wohnsitz_immobilienverm_hh_e"] = apply_lowest_float_dtype(raw_data["p010he"])
    out["finanzverm_hh_a"] = apply_lowest_float_dtype(raw_data["f010ha"])
    out["finanzverm_hh_b"] = apply_lowest_float_dtype(raw_data["f010hb"])
    out["finanzverm_hh_c"] = apply_lowest_float_dtype(raw_data["f010hc"])
    out["finanzverm_hh_d"] = apply_lowest_float_dtype(raw_data["f010hd"])
    out["finanzverm_hh_e"] = apply_lowest_float_dtype(raw_data["f010he"])
    out["bruttoverm_hh_a"] = apply_lowest_float_dtype(raw_data["w010ha"])
    out["bruttoverm_hh_b"] = apply_lowest_float_dtype(raw_data["w010hb"])
    out["bruttoverm_hh_c"] = apply_lowest_float_dtype(raw_data["w010hc"])
    out["bruttoverm_hh_d"] = apply_lowest_float_dtype(raw_data["w010hd"])
    out["bruttoverm_hh_e"] = apply_lowest_float_dtype(raw_data["w010he"])
    out["nettoverm_hh_a"] = apply_lowest_float_dtype(raw_data["w011ha"])
    out["nettoverm_hh_b"] = apply_lowest_float_dtype(raw_data["w011hb"])
    out["nettoverm_hh_c"] = apply_lowest_float_dtype(raw_data["w011hc"])
    out["nettoverm_hh_d"] = apply_lowest_float_dtype(raw_data["w011hd"])
    out["nettoverm_hh_e"] = apply_lowest_float_dtype(raw_data["w011he"])
    out["wert_fahrzeuge_a"] = apply_lowest_int_dtype(raw_data["v010ha"])
    out["wert_fahrzeuge_b"] = apply_lowest_int_dtype(raw_data["v010hb"])
    out["wert_fahrzeuge_c"] = apply_lowest_int_dtype(raw_data["v010hc"])
    out["wert_fahrzeuge_d"] = apply_lowest_int_dtype(raw_data["v010hd"])
    out["wert_fahrzeuge_e"] = apply_lowest_int_dtype(raw_data["v010he"])
    out["bruttoverm_inkl_fahrz_hh_a"] = apply_lowest_float_dtype(raw_data["n010ha"])
    out["bruttoverm_inkl_fahrz_hh_b"] = apply_lowest_float_dtype(raw_data["n010hb"])
    out["bruttoverm_inkl_fahrz_hh_c"] = apply_lowest_float_dtype(raw_data["n010hc"])
    out["bruttoverm_inkl_fahrz_hh_d"] = apply_lowest_float_dtype(raw_data["n010hd"])
    out["bruttoverm_inkl_fahrz_hh_e"] = apply_lowest_float_dtype(raw_data["n010he"])
    out["nettoverm_fahrz_kredit_hh_a"] = apply_lowest_float_dtype(raw_data["n011ha"])
    out["nettoverm_fahrz_kredit_hh_b"] = apply_lowest_float_dtype(raw_data["n011hb"])
    out["nettoverm_fahrz_kredit_hh_c"] = apply_lowest_float_dtype(raw_data["n011hc"])
    out["nettoverm_fahrz_kredit_hh_d"] = apply_lowest_float_dtype(raw_data["n011hd"])
    out["nettoverm_fahrz_kredit_hh_e"] = apply_lowest_float_dtype(raw_data["n011he"])
    out["flag_netwealth"] = str_categorical(
        raw_data["n022h0"],
        ordered=False,
    )

    return hwealth_wide_to_long(out)
