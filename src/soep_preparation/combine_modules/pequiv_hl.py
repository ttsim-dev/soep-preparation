"""Combine variables from the modules pequiv and hl."""

import pandas as pd


def combine(pequiv: pd.DataFrame, hl: pd.DataFrame) -> pd.DataFrame:
    """Combine variables from the cleaned pequiv and hl modules.

    Args:
        pequiv: Cleaned pequiv module.
        hl: Cleaned hl module.

    Returns:
        Combined pequiv and hl modules. If contents conflict with each other,
             the one from pequiv takes precedence.
    """
    merged = pd.merge(left=pequiv, right=hl, on=["hh_id", "survey_year"], how="outer")
    out = pd.DataFrame(index=merged.index)
    out["p_id"] = merged["p_id"]
    out["hh_id"] = merged["hh_id"]
    out["survey_year"] = merged["survey_year"]

    out["arbeitslosengeld_2_m_hh"] = merged[
        "arbeitslosengeld_2_m_hh_pequiv"
    ].combine_first(merged["arbeitslosengeld_2_m_hh_hl"])
    out["kindergeld_m_hh"] = merged["kindergeld_m_hh_pequiv"].combine_first(
        merged["kindergeld_m_hh_hl"]
    )
    out["kinderzuschlag_m_hh"] = merged["kinderzuschlag_m_hh_pequiv"].combine_first(
        merged["kinderzuschlag_m_hh_hl"]
    )
    out["wohngeld_m_hh"] = merged["wohngeld_m_hh_pequiv"].combine_first(
        merged["wohngeld_m_hh_hl"]
    )

    return out
