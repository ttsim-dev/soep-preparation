"""Clean and convert SOEP hl variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_lowest_int_dtype,
    object_to_bool_categorical,
    object_to_float,
    object_to_int,
)


def _kindergeld_hh_betrag_m(
    betrag: "pd.Series[pd.Categorical]",
    bezug: "pd.Series[pd.Categorical]",
) -> "pd.Series[int]":
    out = object_to_int(betrag)
    return out.where(
        ~(betrag.isna()) & (bezug.astype("bool[pyarrow]")),
        0,
    )


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the hl file.

    Args:
        raw_data: The raw hl data.

    Returns:
        The processed hl data.
    """
    out = pd.DataFrame()
    out["hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["survey_year"] = apply_lowest_int_dtype(raw_data["syear"])

    out["kindergeld_hh_bezug_aktuell"] = object_to_bool_categorical(
        raw_data["hlc0044_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["kindergeld_hh_betrag_m_aktuell"] = _kindergeld_hh_betrag_m(
        raw_data["hlc0045_h"],
        out["kindergeld_hh_bezug_aktuell"],
    )
    out["kindergeld_hh_betrag_m_hl"] = object_to_int(raw_data["hlc0042_h"])

    out["kinderzuschlag_hh_bezug_aktuell"] = object_to_bool_categorical(
        raw_data["hlc0046_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["kinderzuschlag_hh_betrag_m_aktuell"] = object_to_int(raw_data["hlc0047_h"])
    out["kinderzuschlag_hh_bezug"] = object_to_bool_categorical(
        raw_data["hlc0049_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["kinderzuschlag_hh_betrag_m_hl"] = object_to_int(raw_data["hlc0051_h"])

    # alg2-variables contain Arbeitslosengeld II, Sozialgeld, and Unterkunftskosten
    out["alg2_hh_bezug_aktuell"] = object_to_bool_categorical(
        raw_data["hlc0064_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["alg2_hh_bezug_anzahl_monate"] = object_to_int(raw_data["hlc0053"])
    out["alg2_hh_betrag_m_hl"] = object_to_float(raw_data["hlc0054"])

    out["hilfe_lebensunterhalt_hh_aktuell"] = object_to_bool_categorical(
        raw_data["hlc0067_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["wohngeld_hh_bezug_aktuell"] = object_to_bool_categorical(
        raw_data["hlc0083_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["wohngeld_hh_betrag_m_hl"] = object_to_int(raw_data["hlc0082_h"])
    out["grundsicherung_im_alter_hh_betrag_m_aktuell"] = object_to_int(
        raw_data["hlc0071"]
    )
    return out
