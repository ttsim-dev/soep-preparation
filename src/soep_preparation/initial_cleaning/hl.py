import pandas as pd

from soep_preparation.utilities import (
    bool_categorical,
    float_categorical_to_float,
    int_categorical_to_int,
)


def _kindergeld_aktuell_hl_m_hh(
    aktuell: "pd.Series[pd.Categorical]",
    bezug_aktuell: "pd.Series[pd.Categorical]",
) -> "pd.Series[int]":
    out = int_categorical_to_int(aktuell)
    return out.where(
        ~(aktuell.isnull()) & (bezug_aktuell.astype("bool[pyarrow]")),
        0,
    )


def clean(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the hl dataset."""
    out = pd.DataFrame()
    out["hh_id"] = int_categorical_to_int(raw["hid"])
    out["year"] = int_categorical_to_int(raw["syear"])

    out["kindergeld_hl_m_hh_prev"] = int_categorical_to_int(raw["hlc0042_h"])
    out["kindergeld_bezug_aktuell"] = bool_categorical(
        raw["hlc0044_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["kindergeld_aktuell_hl_m_hh"] = _kindergeld_aktuell_hl_m_hh(
        raw["hlc0045_h"],
        out["kindergeld_bezug_aktuell"],
    )
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
