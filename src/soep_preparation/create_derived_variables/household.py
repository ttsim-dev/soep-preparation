"""Module to combine household variables from multiple sources."""

import pandas as pd

from soep_preparation.utilities.dataframe_manipulator import (
    combine_first_and_make_categorical,
)


def derive_hh_soep_sample(hpathl: pd.DataFrame, design: pd.DataFrame) -> pd.DataFrame:
    """Merge the hh_soep_sample variable from hpathl and design.

    Args:
        hpathl: DataFrame containing the cleaned hpathl data.
        design: DataFrame containing the cleaned design data.

    Returns:
        DataFrame containing the merged hh_soep_sample variable.
    """
    out = pd.DataFrame(index=hpathl.index)
    merged = pd.merge(hpathl, design, on=["hh_id"], how="outer")
    out["hh_id"] = hpathl["hh_id"].copy()
    out["hh_soep_sample"] = combine_first_and_make_categorical(
        merged,
        "hh_soep_sample_hpathl",
        "hh_soep_sample_design",
        ordered=False,
    )
    return out


def derive_hh_received_transfers(
    pequiv: pd.DataFrame, hl: pd.DataFrame
) -> pd.DataFrame:
    """Merge the household received transfer variables from pequiv and hl.

    Args:
        pequiv: DataFrame containing the cleaned pequiv data.
        hl: DataFrame containing the cleaned hl data.

    Returns:
        DataFrame containing the merged received transfer variables.
    """
    out = pd.DataFrame(index=pequiv.index)
    merged = pd.merge(pequiv, hl, on=["hh_id", "survey_year"], how="outer")
    out[["p_id", "hh_id", "survey_year"]] = pequiv[
        ["p_id", "hh_id", "survey_year"]
    ].copy()
    out["alg2_hh_betrag_m"] = combine_first_and_make_categorical(
        merged,
        "alg2_hh_betrag_m_pequiv",
        "alg2_hh_betrag_m_hl",
        ordered=False,
    )
    out["kindergeld_hh_betrag_m"] = combine_first_and_make_categorical(
        merged,
        "kindergeld_hh_betrag_m_pequiv",
        "kindergeld_hh_betrag_m_hl",
        ordered=False,
    )
    out["kinderzuschlag_hh_betrag_m"] = combine_first_and_make_categorical(
        merged,
        "kinderzuschlag_hh_betrag_m_pequiv",
        "kinderzuschlag_hh_betrag_m_hl",
        ordered=False,
    )
    out["wohngeld_hh_betrag_y"] = combine_first_and_make_categorical(
        merged,
        "wohngeld_hh_betrag_m_pequiv",
        "wohngeld_hh_betrag_m_hl",
        ordered=False,
    )

    return out
