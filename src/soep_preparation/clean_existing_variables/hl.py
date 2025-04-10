"""Functions to pre-process variables for a raw hl dataset."""

import pandas as pd

from soep_preparation.utilities import (
    apply_lowest_int_dtype,
    object_to_bool_categorical,
    object_to_float,
    object_to_int,
)


def _kindergeld_aktuell_hl_m_hh(
    aktuell: "pd.Series[pd.Categorical]",
    bezug_aktuell: "pd.Series[pd.Categorical]",
) -> "pd.Series[int]":
    out = object_to_int(aktuell)
    return out.where(
        ~(aktuell.isna()) & (bezug_aktuell.astype("bool[pyarrow]")),
        0,
    )


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the hl dataset."""
    out = pd.DataFrame()
    out["hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["survey_year"] = apply_lowest_int_dtype(raw_data["syear"])

    out["kindergeld_hl_m_hh_prev"] = object_to_int(raw_data["hlc0042_h"])
    out["kindergeld_bezug_aktuell"] = object_to_bool_categorical(
        raw_data["hlc0044_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["kindergeld_aktuell_hl_m_hh"] = _kindergeld_aktuell_hl_m_hh(
        raw_data["hlc0045_h"],
        out["kindergeld_bezug_aktuell"],
    )
    out["kinderzuschlag_hl_m_hh"] = object_to_int(raw_data["hlc0047_h"])
    out["kinderzuschlag_hl_m_hh_prev"] = object_to_int(raw_data["hlc0051_h"])
    out["alg2_months_soep_hh_prev"] = object_to_int(raw_data["hlc0053"])
    out["arbeitsl_geld_2_soep_m_hh_prev"] = object_to_float(
        raw_data["hlc0054"],
    )
    out["betreu_kosten_pro_kind"] = object_to_bool_categorical(
        raw_data["hlc0009"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["kinderzuschlag_aktuell_hh"] = object_to_bool_categorical(
        raw_data["hlc0046_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["kinderzuschlag_hl_hh_prev"] = object_to_bool_categorical(
        raw_data["hlc0049_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["alg2_etc_aktuell_hh"] = object_to_bool_categorical(
        raw_data["hlc0064_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["hilfe_lebensunterh_aktuell_hh"] = object_to_bool_categorical(
        raw_data["hlc0067_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["wohngeld_soep_m_hh_prev"] = object_to_int(raw_data["hlc0082_h"])
    out["wohngeld_aktuell_hh"] = object_to_bool_categorical(
        raw_data["hlc0083_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    return out
