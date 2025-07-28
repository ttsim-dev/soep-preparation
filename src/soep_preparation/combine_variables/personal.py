"""Module to combine personal variables from multiple sources."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    combine_first_and_make_categorical,
)


def derive_birth_month(ppathl: pd.DataFrame, bioedu: pd.DataFrame) -> pd.DataFrame:
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
    out["birth_month"] = combine_first_and_make_categorical(
        series_1=merged["birth_month_ppathl"],
        series_2=merged["birth_month_bioedu"],
        ordered=False,
    )
    return out


def derive_medical_variables(pequiv: pd.DataFrame, pl: pd.DataFrame) -> pd.DataFrame:
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
    return out


def derive_p_received_transfers(pl: pd.DataFrame, pkal: pd.DataFrame) -> pd.DataFrame:
    """Merge the personal received transfer variables from pl and pkal.

    Args:
        pl: Cleaned pl data.
        pkal: Cleaned pkal data.

    Returns:
        Merged received transfer variables.
    """
    out = pd.DataFrame(index=pl.index)
    merged = pd.merge(pl, pkal, on=["hh_id", "survey_year"], how="outer")
    out[["p_id", "hh_id", "survey_year"]] = pl[["p_id", "hh_id", "survey_year"]].copy()
    out["bezog_mutterschaftsgeld"] = combine_first_and_make_categorical(
        series_1=merged["bezog_mutterschaftsgeld_pl"],
        series_2=merged["bezog_mutterschaftsgeld_pkal"],
        ordered=False,
    )
    return out
