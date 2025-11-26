"""Combine variables from the modules pequiv and pl."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    combine_first_and_make_categorical,
)


def combine(pequiv: pd.DataFrame, pl: pd.DataFrame) -> pd.DataFrame:
    """Combine variables from the cleaned pequiv and pl modules.

    Args:
        pequiv: Cleaned pequiv module.
        pl: Cleaned pl module.

    Returns:
        Combined pequiv and pl modules.
    """
    merged = pd.merge(
        left=pequiv,
        right=pl,
        on=["p_id", "hh_id", "survey_year"],
        how="outer",
    )
    out = pd.DataFrame(index=merged.index)
    out[["p_id", "hh_id", "survey_year", "hh_id_original"]] = merged[
        ["p_id", "hh_id", "survey_year", "hh_id_original"]
    ].copy()

    out["med_schw_treppen"] = combine_first_and_make_categorical(
        series_1=merged["med_schwierigkeiten_treppen_pequiv"],
        series_2=merged["med_schwierigkeit_treppen_pl"],
        ordered=True,
    )
    out["med_bluthochdruck"] = combine_first_and_make_categorical(
        series_1=merged["med_bluthochdruck_pequiv"],
        series_2=merged["med_bluthochdruck_pl"],
        ordered=True,
    )
    out["med_diabetes"] = combine_first_and_make_categorical(
        series_1=merged["med_diabetes_pequiv"],
        series_2=merged["med_diabetes_pl"],
        ordered=True,
    )
    out["med_krebs"] = combine_first_and_make_categorical(
        series_1=merged["med_krebs_pequiv"],
        series_2=merged["med_krebs_pl"],
        ordered=True,
    )
    out["med_herzkrankheit"] = combine_first_and_make_categorical(
        series_1=merged["med_herzkrankheit_pequiv"],
        series_2=merged["med_herzkrankheit_pl"],
        ordered=True,
    )
    out["med_schlaganfall"] = combine_first_and_make_categorical(
        series_1=merged["med_schlaganfall_pequiv"],
        series_2=merged["med_schlaganfall_pl"],
        ordered=True,
    )
    out["med_gelenk"] = combine_first_and_make_categorical(
        series_1=merged["med_gelenk_pequiv"],
        series_2=merged["med_gelenk_pl"],
        ordered=True,
    )
    out["med_gewicht"] = merged["med_gewicht_pequiv"].combine_first(
        merged["med_gewicht_pl"]
    )
    out["med_größe"] = merged["med_größe_pequiv"].combine_first(merged["med_größe_pl"])
    out["bmi"] = merged["bmi_pequiv"].combine_first(merged["bmi_pl"])
    out["obese"] = merged["obese_pequiv"].combine_first(merged["obese_pl"])
    out["med_subjective_status"] = merged["med_subjective_status_pequiv"].combine_first(
        merged["med_subjective_status_pl"]
    )
    out["med_subjective_status_dummy"] = merged[
        "med_subjective_status_dummy_pequiv"
    ].combine_first(merged["med_subjective_status_dummy_pl"])
    out["frailty"] = merged["frailty_pequiv"].combine_first(merged["frailty_pl"])

    out["kindesunterhalt_erhalten_m"] = merged[
        "kindesunterhalt_erhalten_m_pequiv"
    ].combine_first(merged["kindesunterhalt_erhalten_m_pl"])

    return out
