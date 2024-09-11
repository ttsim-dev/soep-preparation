from soep_cleaning.config import pd
from soep_cleaning.initial_cleaning.helper import (
    bool_categorical,
    float_categorical_to_float,
    float_categorical_to_int,
    hwealth_wide_to_long,
    int_categorical_to_int,
    int_to_int_categorical,
    str_categorical,
)
from soep_cleaning.utilities import apply_lowest_float_dtype, apply_lowest_int_dtype


def hgen(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the hgen dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = apply_lowest_int_dtype(raw["cid"])
    out["soep_hh_id"] = apply_lowest_int_dtype(raw["hid"])

    out["year"] = apply_lowest_int_dtype(raw["syear"])
    out["building_year_hh_max"] = int_to_int_categorical(
        float_categorical_to_int(raw["hgcnstyrmax"]),
    )
    out["building_year_hh_min"] = int_to_int_categorical(
        float_categorical_to_int(raw["hgcnstyrmin"]),
    )
    out["heating_costs_m_hh"] = (float_categorical_to_float(raw["hgheat"]),)
    out["einzugsjahr"] = int_to_int_categorical(
        float_categorical_to_int(raw["hgmoveyr"]),
    )
    out["bruttokaltmiete_m_hh"] = float_categorical_to_float(raw["hgrent"])
    out["living_space_hh"] = int_to_int_categorical(
        float_categorical_to_int(raw["hgsize"]),
    )
    out["heizkosten_mi_reason"] = str_categorical(
        raw["hgheatinfo"],
        ordered=False,
    )
    out["rented_or_owned"] = str_categorical(
        raw["hgowner"],
        ordered=False,
    )
    out["hh_typ"] = str_categorical(
        raw["hgtyp1hh"],
        nr_identifiers=2,
        ordered=False,
    )
    out["hh_typ_2st"] = str_categorical(
        raw["hgtyp2hh"],
        ordered=False,
    )
    return out


def hl(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the hl dataset."""
    out = pd.DataFrame()
    out["soep_hh_id"] = int_categorical_to_int(raw["hid"])
    out["year"] = int_categorical_to_int(raw["syear"])

    out["kindergeld_hl_m_hh_prev"] = int_categorical_to_int(raw["hlc0042_h"])
    out["kindergeld_aktuell_hl_m_hh"] = int_categorical_to_int(raw["hlc0045_h"])
    out["kinderzuschlag_hl_m_hh"] = int_categorical_to_int(raw["hlc0047_h"])
    out["kinderzuschlag_hl_m_hh_prev"] = int_categorical_to_int(raw["hlc0051_h"])
    out["alg2_months_soep_hh_prev"] = int_categorical_to_int(raw["hlc0053"])
    out["arbeitsl_geld_2_soep_m_hh_prev"] = float_categorical_to_float(
        raw["hlc0054"],
    )

    out["betreu_kosten_pro_kind"] = bool_categorical(
        raw["hlc0009"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["kindergeld_bezug_aktuell"] = bool_categorical(
        raw["hlc0044_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["kinderzuschlag_aktuell_hh"] = bool_categorical(
        raw["hlc0046_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["kinderzuschlag_hl_hh_prev"] = bool_categorical(
        raw["hlc0049_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["alg2_etc_aktuell_hh"] = bool_categorical(
        raw["hlc0064_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["hilfe_lebensunterh_aktuell_hh"] = bool_categorical(
        raw["hlc0067_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["wohngeld_soep_m_hh_prev"] = int_categorical_to_int(raw["hlc0082_h"])
    out["wohngeld_aktuell_hh"] = bool_categorical(
        raw["hlc0083_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    return out


def hpathl(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the hpathl dataset."""
    out = pd.DataFrame()
    out["soep_hh_id"] = apply_lowest_int_dtype(raw["hid"])
    out["year"] = apply_lowest_int_dtype(raw["syear"])
    out["hh_soep_sample"] = str_categorical(
        raw["hsample"],
        ordered=False,
    )  # TODO: Remove the additional alphabetical identifier level?
    out["hh_bleibe_wkeit"] = apply_lowest_float_dtype(raw["hbleib"])
    out["hh_gewicht"] = apply_lowest_float_dtype(raw["hhrf"])
    out["hh_gewicht_nur_neue"] = apply_lowest_float_dtype(raw["hhrf0"])
    out["hh_gewicht_ohne_neue"] = apply_lowest_float_dtype(raw["hhrf0"])
    return out


def hwealth(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the hwealth dataset."""
    out = pd.DataFrame()
    out["year"] = apply_lowest_int_dtype(raw["syear"])
    out["soep_hh_id"] = apply_lowest_int_dtype(raw["hid"])

    out["wohnsitz_immobilienverm_hh_a"] = apply_lowest_float_dtype(raw["p010ha"])
    out["wohnsitz_immobilienverm_hh_b"] = apply_lowest_float_dtype(raw["p010hb"])
    out["wohnsitz_immobilienverm_hh_c"] = apply_lowest_float_dtype(raw["p010hc"])
    out["wohnsitz_immobilienverm_hh_d"] = apply_lowest_float_dtype(raw["p010hd"])
    out["wohnsitz_immobilienverm_hh_e"] = apply_lowest_float_dtype(raw["p010he"])

    out["finanzverm_hh_a"] = apply_lowest_float_dtype(raw["f010ha"])
    out["finanzverm_hh_b"] = apply_lowest_float_dtype(raw["f010hb"])
    out["finanzverm_hh_c"] = apply_lowest_float_dtype(raw["f010hc"])
    out["finanzverm_hh_d"] = apply_lowest_float_dtype(raw["f010hd"])
    out["finanzverm_hh_e"] = apply_lowest_float_dtype(raw["f010he"])

    out["bruttoverm_hh_a"] = apply_lowest_float_dtype(raw["w010ha"])
    out["bruttoverm_hh_b"] = apply_lowest_float_dtype(raw["w010hb"])
    out["bruttoverm_hh_c"] = apply_lowest_float_dtype(raw["w010hc"])
    out["bruttoverm_hh_d"] = apply_lowest_float_dtype(raw["w010hd"])
    out["bruttoverm_hh_e"] = apply_lowest_float_dtype(raw["w010he"])

    out["nettoverm_hh_a"] = apply_lowest_float_dtype(raw["w011ha"])
    out["nettoverm_hh_b"] = apply_lowest_float_dtype(raw["w011hb"])
    out["nettoverm_hh_c"] = apply_lowest_float_dtype(raw["w011hc"])
    out["nettoverm_hh_d"] = apply_lowest_float_dtype(raw["w011hd"])
    out["nettoverm_hh_e"] = apply_lowest_float_dtype(raw["w011he"])

    out["wert_fahrzeuge_a"] = apply_lowest_int_dtype(raw["v010ha"])
    out["wert_fahrzeuge_b"] = apply_lowest_int_dtype(raw["v010hb"])
    out["wert_fahrzeuge_c"] = apply_lowest_int_dtype(raw["v010hc"])
    out["wert_fahrzeuge_d"] = apply_lowest_int_dtype(raw["v010hd"])
    out["wert_fahrzeuge_e"] = apply_lowest_int_dtype(raw["v010he"])

    out["bruttoverm_inkl_fahrz_hh_a"] = apply_lowest_float_dtype(raw["n010ha"])
    out["bruttoverm_inkl_fahrz_hh_b"] = apply_lowest_float_dtype(raw["n010hb"])
    out["bruttoverm_inkl_fahrz_hh_c"] = apply_lowest_float_dtype(raw["n010hc"])
    out["bruttoverm_inkl_fahrz_hh_d"] = apply_lowest_float_dtype(raw["n010hd"])
    out["bruttoverm_inkl_fahrz_hh_e"] = apply_lowest_float_dtype(raw["n010he"])

    out["nettoverm_fahrz_kredit_hh_a"] = apply_lowest_float_dtype(raw["n011ha"])
    out["nettoverm_fahrz_kredit_hh_b"] = apply_lowest_float_dtype(raw["n011hb"])
    out["nettoverm_fahrz_kredit_hh_c"] = apply_lowest_float_dtype(raw["n011hc"])
    out["nettoverm_fahrz_kredit_hh_d"] = apply_lowest_float_dtype(raw["n011hd"])
    out["nettoverm_fahrz_kredit_hh_e"] = apply_lowest_float_dtype(raw["n011he"])

    out["flag_netwealth"] = str_categorical(
        raw["n022h0"],
        ordered=False,
    )
    return hwealth_wide_to_long(out)
