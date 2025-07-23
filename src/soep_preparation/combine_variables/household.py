"""Module to combine household variables from multiple sources."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    combine_first_and_make_categorical,
)


def derive_hh_soep_sample(hpathl: pd.DataFrame, design: pd.DataFrame) -> pd.DataFrame:
    """Merge the hh_soep_sample variable from hpathl and design.

    Args:
        hpathl: Cleaned hpathl data.
        design: Cleaned design data.

    Returns:
        Merged hh_soep_sample variable.
    """
    out = pd.DataFrame(index=hpathl.index)
    merged = pd.merge(hpathl, design, on=["hh_id"], how="outer")
    out["hh_id"] = hpathl["hh_id"].copy()
    out["hh_soep_sample"] = combine_first_and_make_categorical(
        series_1=merged["hh_soep_sample_hpathl"],
        series_2=merged["hh_soep_sample_design"],
        ordered=False,
    )
    return out


def derive_hh_received_transfers(
    pequiv: pd.DataFrame, hl: pd.DataFrame
) -> pd.DataFrame:
    """Merge the household received transfer variables from pequiv and hl.

    Args:
        pequiv: Cleaned pequiv data.
        hl: Cleaned hl data.

    Returns:
        Merged received transfer variables.
    """
    out = pd.DataFrame(index=pequiv.index)
    merged = pd.merge(pequiv, hl, on=["hh_id", "survey_year"], how="outer")
    out[["p_id", "hh_id", "survey_year"]] = pequiv[
        ["p_id", "hh_id", "survey_year"]
    ].copy()
    out["arbeitslosengeld_2_betrag_m_hh"] = combine_first_and_make_categorical(
        series_1=merged["arbeitslosengeld_2_betrag_m_hh_pequiv"],
        series_2=merged["arbeitslosengeld_2_betrag_m_hh_hl"],
        ordered=False,
    )
    out["kindergeld_betrag_m_hh"] = combine_first_and_make_categorical(
        series_1=merged["kindergeld_betrag_m_hh_pequiv"],
        series_2=merged["kindergeld_betrag_m_hh_hl"],
        ordered=False,
    )
    out["kinderzuschlag_betrag_m_hh"] = combine_first_and_make_categorical(
        series_1=merged["kinderzuschlag_betrag_m_hh_pequiv"],
        series_2=merged["kinderzuschlag_betrag_m_hh_hl"],
        ordered=False,
    )
    out["wohngeld_betrag_m_hh"] = combine_first_and_make_categorical(
        series_1=merged["wohngeld_betrag_m_hh_pequiv"],
        series_2=merged["wohngeld_betrag_m_hh_hl"],
        ordered=False,
    )

    return out
