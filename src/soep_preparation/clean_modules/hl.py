"""Clean and convert SOEP hl variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    apply_smallest_int_dtype,
    object_to_bool_categorical,
    object_to_float,
    object_to_int,
)


def _kindergeld_m_hh(
    betrag: pd.Series[pd.Categorical],
    bezug: pd.Series[pd.Categorical],
) -> pd.Series[int]:
    out = object_to_int(betrag)
    return out.where(
        ~(betrag.isna()) & (bezug.astype("bool[pyarrow]")),
        0,
    )


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned variables from the hl module.

    Args:
        raw_data: The raw hl data.

    Returns:
        The processed hl data.
    """
    out = pd.DataFrame()
    out["hh_id"] = apply_smallest_int_dtype(raw_data["hid"])
    out["survey_year"] = apply_smallest_int_dtype(raw_data["syear"])

    out["bezieht_aktuell_kindergeld_hh"] = object_to_bool_categorical(
        series=raw_data["hlc0044_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["kindergeld_m_aktuell_hh"] = _kindergeld_m_hh(
        betrag=raw_data["hlc0045_h"],
        bezug=out["bezieht_aktuell_kindergeld_hh"],
    )
    out["kindergeld_m_hh_hl"] = object_to_int(raw_data["hlc0042_h"])

    out["bezieht_aktuell_kinderzuschlag_hh"] = object_to_bool_categorical(
        series=raw_data["hlc0046_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["kinderzuschlag_m_aktuell_hh"] = object_to_int(raw_data["hlc0047_h"])
    out["bezog_kinderzuschlag_hh"] = object_to_bool_categorical(
        raw_data["hlc0049_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["kinderzuschlag_m_hh_hl"] = object_to_int(raw_data["hlc0051_h"])

    out["bezieht_aktuell_wohngeld_hh"] = object_to_bool_categorical(
        series=raw_data["hlc0083_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["wohngeld_m_hh_hl"] = object_to_int(raw_data["hlc0082_h"])

    # Variables contain Arbeitslosengeld II Bedarf, Sozialgeld, Kosten der Unterkunft
    out["bezieht_aktuell_arbeitslosengeld_2_hh"] = object_to_bool_categorical(
        series=raw_data["hlc0064_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["arbeitslosengeld_2_anzahl_monate_hh"] = object_to_int(raw_data["hlc0053"])
    out["arbeitslosengeld_2_m_hh_hl"] = object_to_float(raw_data["hlc0054"])

    out["bezieht_aktuell_hilfe_zum_lebensunterhalt_hh"] = object_to_bool_categorical(
        series=raw_data["hlc0067_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )

    out["grundsicherung_im_alter_m_aktuell_hh"] = object_to_int(
        series=raw_data["hlc0071"]
    )
    return out
