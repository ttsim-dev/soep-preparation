"""Module to combine personal variables from multiple sources."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    combined_categorical,
)


def combine_birth_month(ppathl: pd.DataFrame, bioedu: pd.DataFrame) -> pd.DataFrame:
    """Combine the birth_month variables from ppathl and bioedu.

    Args:
        ppathl: Cleaned ppathl data.
        bioedu: Cleaned bioedu data.

    Returns:
        Combined birth_month variable.
    """
    out = pd.DataFrame()
    merged = pd.merge(left=ppathl, right=bioedu, on="p_id", how="outer")
    out["p_id"] = merged["p_id"].unique()
    out["birth_month"] = combined_categorical(
        series_1=merged["birth_month_ppathl"],
        series_2=merged["birth_month_bioedu"],
        ordered=False,
    )
    return out


def combine_medical_variables(pequiv: pd.DataFrame, pl: pd.DataFrame) -> pd.DataFrame:
    """Combine the medical variables from pequiv and pl.

    Args:
        pequiv: Cleaned pequiv data.
        pl: Cleaned pl data.

    Returns:
        Combined medical variables.
    """
    out = pd.DataFrame(index=pequiv.index)
    merged = pd.merge(pequiv, pl, on=["p_id", "hh_id", "survey_year"], how="outer")
    out[["p_id", "hh_id", "survey_year", "hh_id_original"]] = merged[
        ["p_id", "hh_id", "survey_year", "hh_id_original"]
    ].copy()

    out["med_schw_treppen"] = combined_categorical(
        series_1=merged["med_schwierigkeiten_treppen_pequiv"],
        series_2=merged["med_schwierigkeit_treppen_pl"],
        ordered=True,
    )
    out["med_bluthochdruck"] = combined_categorical(
        series_1=merged["med_bluthochdruck_pequiv"],
        series_2=merged["med_bluthochdruck_pl"],
        ordered=True,
    )
    out["med_diabetes"] = combined_categorical(
        series_1=merged["med_diabetes_pequiv"],
        series_2=merged["med_diabetes_pl"],
        ordered=True,
    )
    out["med_krebs"] = combined_categorical(
        series_1=merged["med_krebs_pequiv"],
        series_2=merged["med_krebs_pl"],
        ordered=True,
    )
    out["med_herzkrankheit"] = combined_categorical(
        series_1=merged["med_herzkrankheit_pequiv"],
        series_2=merged["med_herzkrankheit_pl"],
        ordered=True,
    )
    out["med_schlaganfall"] = combined_categorical(
        series_1=merged["med_schlaganfall_pequiv"],
        series_2=merged["med_schlaganfall_pl"],
        ordered=True,
    )
    out["med_gelenk"] = combined_categorical(
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
    return out


def combine_p_bezog_mutterschaftsgeld(
    pl: pd.DataFrame, pkal: pd.DataFrame
) -> pd.DataFrame:
    """Merge the personal bezog_mutterschaftsgeld variable from pl and pkal.

    Args:
        pl: Cleaned pl data.
        pkal: Cleaned pkal data.

    Returns:
        Merged bezog_mutterschaftsgeld variable.
    """
    out = pd.DataFrame(index=pl.index)
    merged = pd.merge(pl, pkal, on=["hh_id", "survey_year"], how="outer")
    out[["p_id", "hh_id", "survey_year"]] = pl[["p_id", "hh_id", "survey_year"]].copy()
    out["bezog_mutterschaftsgeld"] = combined_categorical(
        series_1=merged["bezog_mutterschaftsgeld_pl"],
        series_2=merged["bezog_mutterschaftsgeld_pkal"],
        ordered=False,
    )
    return out


def combine_p_kindesunterhalt_erhalten(
    pl: pd.DataFrame, pequiv: pd.DataFrame
) -> pd.DataFrame:
    """Merge the personal kindesunterhalt_erhalten variable from pl and pequiv.

    Args:
        pl: Cleaned pl data.
        pequiv: Cleaned pequiv data.

    Returns:
        Merged kindesunterhalt_erhalten variable.
    """
    out = pd.DataFrame(index=pl.index)
    merged = pd.merge(pl, pequiv, on=["hh_id", "survey_year"], how="outer")
    out[["p_id", "hh_id", "survey_year"]] = pl[["p_id", "hh_id", "survey_year"]].copy()
    out["kindesunterhalt_erhalten_m_pequiv"] = combined_categorical(
        series_1=merged["kindesunterhalt_erhalten_m_pl"],
        series_2=merged["kindesunterhalt_erhalten_m_pequiv"],
        ordered=False,
    )
    return out
