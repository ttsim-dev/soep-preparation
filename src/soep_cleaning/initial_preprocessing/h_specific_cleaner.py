import pandas as pd

from soep_cleaning.initial_preprocessing.helper import (
    bool_categorical,
    int_categorical,
    int_categorical_to_int,
    str_categorical,
)
from soep_cleaning.utilities import find_lowest_float_dtype, find_lowest_int_dtype


def hgen(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = int_categorical_to_int(raw_data["cid"])
    out["soep_hh_id"] = raw_data["hid"].astype(find_lowest_int_dtype(raw_data["hid"]))

    out["year"] = int_categorical_to_int(raw_data["syear"])
    out["building_year_hh_max"] = raw_data["hgcnstyrmax"].astype(
        find_lowest_int_dtype(raw_data["hgcnstyrmax"]),
    )
    out["building_year_hh_min"] = raw_data["hgcnstyrmin"].astype(
        find_lowest_int_dtype(raw_data["hgcnstyrmin"]),
    )
    out["heating_costs_m_hh"] = raw_data["hgheat"].astype(
        find_lowest_int_dtype(raw_data["hgheat"]),
    )
    out["einzugsjahr"] = int_categorical(raw_data["hgmoveyr"])
    out["bruttokaltmiete_m_hh"] = raw_data["hgrent"].astype(
        find_lowest_int_dtype(raw_data["hgrent"]),
    )
    out["living_space_hh"] = raw_data["hgsize"].astype(
        find_lowest_int_dtype(raw_data["hgsize"]),
    )

    out["heizkosten_mi_reason"] = str_categorical(
        raw_data["hgheatinfo"],
        unordered=True,
    )
    out["rented_or_owned"] = str_categorical(
        raw_data["hgowner"],
        unordered=True,
    )
    out["hh_typ"] = str_categorical(
        sr=raw_data["hgtyp1hh"],
        no_identifiers=2,
        unordered=True,
    )
    out["hh_typ_2st"] = str_categorical(
        raw_data["hgtyp2hh"],
        unordered=True,
    )

    return out


def hl(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["soep_hh_id"] = raw_data["hid"].astype(find_lowest_int_dtype(raw_data["hid"]))

    out["year"] = int_categorical_to_int(raw_data["syear"])

    out["kindergeld_hl_m_hh_prev"] = raw_data["hlc0042_h"].astype(
        find_lowest_int_dtype(raw_data["hlc0042_h"]),
    )
    out["kindergeld_aktuell_hl_m_hh"] = raw_data["hlc0045_h"].astype(
        find_lowest_int_dtype(raw_data["hlc0045_h"]),
    )
    out["kinderzuschlag_hl_m_hh"] = raw_data["hlc0047_h"].astype(
        find_lowest_int_dtype(raw_data["hlc0047_h"]),
    )
    out["kinderzuschlag_hl_m_hh_prev"] = raw_data["hlc0051_h"].astype(
        find_lowest_int_dtype(raw_data["hlc0051_h"]),
    )
    out["alg2_months_soep_hh_prev"] = raw_data["hlc0053"].astype(
        find_lowest_int_dtype(raw_data["hlc0053"]),
    )
    out["arbeitsl_geld_2_soep_m_hh_prev"] = raw_data["hlc0054"].astype(
        find_lowest_int_dtype(raw_data["hlc0054"]),
    )
    out["betreu_kosten_pro_kind"] = raw_data["hld0009"].astype(
        find_lowest_int_dtype(raw_data["hld0009"]),
    )

    out["kindergeld_bezug_aktuell"] = bool_categorical(raw_data["hlc0044_h"])
    out["kinderzuschlag_aktuell_hh"] = bool_categorical(raw_data["hlc0046_h"])
    out["kinderzuschlag_hl_hh_prev"] = bool_categorical(raw_data["hlc0049_h"])
    out["alg2_etc_aktuell_hh"] = bool_categorical(raw_data["hlc0064_h"])
    out["hilfe_lebensunterh_aktuell_hh"] = bool_categorical(
        raw_data["hlc0067_h"],
    )
    out["wohngeld_soep_m_hh_prev"] = int_categorical(raw_data["hlc0082_h"])
    out["wohngeld_aktuell_hh"] = bool_categorical(raw_data["hlc0083_h"])

    return out


def hpathl(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the hpathl dataset."""
    out = pd.DataFrame()
    out["soep_hh_id"] = raw_data["hid"].astype(find_lowest_int_dtype(raw_data["hid"]))
    out["year"] = int_categorical_to_int(raw_data["syear"])
    out["hh_soep_sample"] = str_categorical(
        raw_data["hsample"],
        no_identifiers=2,
        unordered=True,
    )

    out["hh_bleibe_wkeit"] = raw_data["hbleib"].astype(
        find_lowest_float_dtype(raw_data["hbleib"]),
    )
    out["hh_gewicht"] = raw_data["hhrf"].astype(
        find_lowest_float_dtype(raw_data["hhrf"]),
    )
    out["hh_gewicht_nur_neue"] = raw_data["hhrf0"].astype(
        find_lowest_float_dtype(raw_data["hhrf0"]),
    )
    out["hh_gewicht_ohne_neue"] = raw_data["hhrf1"].astype(
        find_lowest_float_dtype(raw_data["hhrf1"]),
    )
    return out


def hwealth(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the hwealth dataset."""
    out = pd.DataFrame()
    out["year"] = int_categorical_to_int(raw_data["syear"])
    out["soep_hh_id"] = raw_data["hid"].astype(find_lowest_int_dtype(raw_data["hid"]))

    out["wohnsitz_immobilienverm_hh_a"] = raw_data["p010ha"].astype(
        find_lowest_float_dtype(raw_data["p010ha"]),
    )
    out["wohnsitz_immobilienverm_hh_b"] = raw_data["p010hb"].astype(
        find_lowest_float_dtype(raw_data["p010hb"]),
    )
    out["wohnsitz_immobilienverm_hh_c"] = raw_data["p010hc"].astype(
        find_lowest_float_dtype(raw_data["p010hc"]),
    )
    out["wohnsitz_immobilienverm_hh_d"] = raw_data["p010hd"].astype(
        find_lowest_float_dtype(raw_data["p010hd"]),
    )
    out["wohnsitz_immobilienverm_hh_e"] = raw_data["p010he"].astype(
        find_lowest_float_dtype(raw_data["p010he"]),
    )
    out["finanzverm_hh_a"] = raw_data["f010ha"].astype(
        find_lowest_float_dtype(raw_data["f010ha"]),
    )
    out["finanzverm_hh_b"] = raw_data["f010hb"].astype(
        find_lowest_float_dtype(raw_data["f010hb"]),
    )
    out["finanzverm_hh_c"] = raw_data["f010hc"].astype(
        find_lowest_float_dtype(raw_data["f010hc"]),
    )
    out["finanzverm_hh_d"] = raw_data["f010hd"].astype(
        find_lowest_float_dtype(raw_data["f010hd"]),
    )
    out["finanzverm_hh_e"] = raw_data["f010he"].astype(
        find_lowest_float_dtype(raw_data["f010he"]),
    )
    out["bruttoverm_hh_a"] = raw_data["w010ha"].astype(
        find_lowest_float_dtype(raw_data["w010ha"]),
    )
    out["bruttoverm_hh_b"] = raw_data["w010hb"].astype(
        find_lowest_float_dtype(raw_data["w010hb"]),
    )
    out["bruttoverm_hh_c"] = raw_data["w010hc"].astype(
        find_lowest_float_dtype(raw_data["w010hc"]),
    )
    out["bruttoverm_hh_d"] = raw_data["w010hd"].astype(
        find_lowest_float_dtype(raw_data["w010hd"]),
    )
    out["bruttoverm_hh_e"] = raw_data["w010he"].astype(
        find_lowest_float_dtype(raw_data["w010he"]),
    )
    out["nettoverm_hh_a"] = raw_data["w011ha"].astype(
        find_lowest_float_dtype(raw_data["w011ha"]),
    )
    out["nettoverm_hh_b"] = raw_data["w011hb"].astype(
        find_lowest_float_dtype(raw_data["w011hb"]),
    )
    out["nettoverm_hh_c"] = raw_data["w011hc"].astype(
        find_lowest_float_dtype(raw_data["w011hc"]),
    )
    out["nettoverm_hh_d"] = raw_data["w011hd"].astype(
        find_lowest_float_dtype(raw_data["w011hd"]),
    )
    out["nettoverm_hh_e"] = raw_data["w011he"].astype(
        find_lowest_float_dtype(raw_data["w011he"]),
    )
    out["wert_fahrzeuge_a"] = raw_data["v010ha"].astype(
        find_lowest_int_dtype(raw_data["v010ha"]),
    )
    out["wert_fahrzeuge_b"] = raw_data["v010hb"].astype(
        find_lowest_int_dtype(raw_data["v010hb"]),
    )
    out["wert_fahrzeuge_c"] = raw_data["v010hc"].astype(
        find_lowest_int_dtype(raw_data["v010hc"]),
    )
    out["wert_fahrzeuge_d"] = raw_data["v010hd"].astype(
        find_lowest_int_dtype(raw_data["v010hd"]),
    )
    out["wert_fahrzeuge_e"] = raw_data["v010he"].astype(
        find_lowest_int_dtype(raw_data["v010he"]),
    )
    out["bruttoverm_inkl_fahrz_hh_a"] = raw_data["n010ha"].astype(
        find_lowest_float_dtype(raw_data["n010ha"]),
    )
    out["bruttoverm_inkl_fahrz_hh_b"] = raw_data["n010hb"].astype(
        find_lowest_float_dtype(raw_data["n010hb"]),
    )
    out["bruttoverm_inkl_fahrz_hh_c"] = raw_data["n010hc"].astype(
        find_lowest_float_dtype(raw_data["n010hc"]),
    )
    out["bruttoverm_inkl_fahrz_hh_d"] = raw_data["n010hd"].astype(
        find_lowest_float_dtype(raw_data["n010hd"]),
    )
    out["bruttoverm_inkl_fahrz_hh_e"] = raw_data["n010he"].astype(
        find_lowest_float_dtype(raw_data["n010he"]),
    )
    out["nettoverm_fahrz_kredit_hh_a"] = raw_data["n011ha"].astype(
        find_lowest_float_dtype(raw_data["n011ha"]),
    )
    out["nettoverm_fahrz_kredit_hh_b"] = raw_data["n011hb"].astype(
        find_lowest_float_dtype(raw_data["n011hb"]),
    )
    out["nettoverm_fahrz_kredit_hh_c"] = raw_data["n011hc"].astype(
        find_lowest_float_dtype(raw_data["n011hc"]),
    )
    out["nettoverm_fahrz_kredit_hh_d"] = raw_data["n011hd"].astype(
        find_lowest_float_dtype(raw_data["n011hd"]),
    )
    out["flag_netwealth"] = str_categorical(
        raw_data["n022h0"],
        unordered=True,
    )
    out["nettoverm_fahrz_kredit_hh_e"] = raw_data["n011he"].astype(
        find_lowest_float_dtype(raw_data["n011he"]),
    )
    # do I need to transform this data?
    return out
