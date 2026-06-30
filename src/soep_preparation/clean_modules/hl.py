"""Clean and convert SOEP hl variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    apply_smallest_int_dtype,
    object_to_bool_categorical,
    object_to_float,
    object_to_int,
    replace_not_applicable_answer,
)


def _kindergeld_m_hh(
    betrag: pd.Series[pd.Categorical],
    bezug: pd.Series[pd.Categorical],
) -> pd.Series[float]:
    out = object_to_float(betrag)
    return out.where(
        ~(betrag.isna()) & (bezug.astype("bool[pyarrow]")),
        0,
    )


def _grundsicherung_im_alter_received(amount_m: pd.Series[float]) -> pd.Series:
    """Receipt of Grundsicherung im Alter, inferred from a positive amount.

    SOEP carries no separate receipt question for Grundsicherung im Alter — only
    the monthly amount (`hlc0071`). A positive amount means the household
    receives it; not-applicable amounts are cleaned to 0 upstream, so they map
    to non-receipt. A genuinely missing amount yields a missing receipt flag.
    """
    return (amount_m > 0).astype("bool[pyarrow]")


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

    out["kindergeld_received_hh"] = object_to_bool_categorical(
        series=raw_data["hlc0044_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["kindergeld_currently_received_m_hh"] = _kindergeld_m_hh(
        betrag=raw_data["hlc0045_h"],
        bezug=out["kindergeld_received_hh"],
    )
    out["kindergeld_m_hh_hl"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["hlc0042_h"], value=0)
    )

    out["kinderzuschlag_currently_received_hh"] = object_to_bool_categorical(
        series=raw_data["hlc0046_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["kinderzuschlag_currently_received_m_hh"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["hlc0047_h"], value=0)
    )
    out["kinderzuschlag_received_last_year_hh"] = object_to_bool_categorical(
        raw_data["hlc0049_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["kinderzuschlag_m_hh_hl"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["hlc0051_h"], value=0)
    )

    out["wohngeld_received_hh"] = object_to_bool_categorical(
        series=raw_data["hlc0083_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["wohngeld_m_hh_hl"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["hlc0082_h"], value=0)
    )

    # Variables contain Arbeitslosengeld II Bedarf, Sozialgeld, Kosten der Unterkunft
    out["arbeitslosengeld_2_currently_received_hh"] = object_to_bool_categorical(
        series=raw_data["hlc0064_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["arbeitslosengeld_2_number_of_months_hh"] = object_to_int(raw_data["hlc0053"])
    out["arbeitslosengeld_2_m_hh_hl"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["hlc0054"], value=0)
    )

    out["hilfe_zum_lebensunterhalt_received_hh"] = object_to_bool_categorical(
        series=raw_data["hlc0067_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )

    out["grundsicherung_im_alter_currently_received_m_hh"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["hlc0071"], value=0)
    )
    out["grundsicherung_im_alter_currently_received_hh"] = (
        _grundsicherung_im_alter_received(
            out["grundsicherung_im_alter_currently_received_m_hh"]
        )
    )
    return out
