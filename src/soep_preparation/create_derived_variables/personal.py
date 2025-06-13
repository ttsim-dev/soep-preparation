"""Module to combine personal variables from multiple sources."""

import pandas as pd

from soep_preparation.utilities.dataframe_manipulator import (
    combine_first_and_make_categorical,
)


def derive_birth_month(ppathl: pd.DataFrame, bioedu: pd.DataFrame) -> pd.DataFrame:
    """Combine the birth_month variables from ppathl and bioedu.

    Args:
        ppathl: DataFrame containing cleaned ppathl data.
        bioedu: DataFrame containing cleaned bioedu data.

    Returns:
        DataFrame containing the combined birth_month variable.
    """
    out = pd.DataFrame()
    merged = pd.merge(left=ppathl, right=bioedu, on="p_id", how="outer")
    out["p_id"] = merged["p_id"].unique()
    out["birth_month"] = merged["birth_month_ppathl"].combine_first(
        merged["birth_month_bioedu"]
    )
    return out


def derive_medical_variables(pequiv: pd.DataFrame, pl: pd.DataFrame) -> pd.DataFrame:
    """Combine the medical variables from pequiv and pl.

    Args:
        pequiv: DataFrame containing cleaned pequiv data.
        pl: DataFrame containing cleaned pl data.

    Returns:
        DataFrame containing the combined medical variables.
    """
    out = pd.DataFrame(index=pequiv.index)
    merged = pd.merge(pequiv, pl, on=["p_id", "hh_id", "survey_year"], how="outer")
    out[["p_id", "hh_id", "survey_year", "hh_id_original"]] = pequiv[
        ["p_id", "hh_id", "survey_year", "hh_id_original"]
    ].copy()

    out["med_schw_treppen"] = combine_first_and_make_categorical(
        merged,
        "med_schwierigkeiten_treppen_pequiv",
        "med_schwierigkeit_treppen_dummy_pl",
        ordered=True,
    )
    out["med_bluthochdruck"] = combine_first_and_make_categorical(
        merged,
        "med_bluthochdruck_pequiv",
        "med_bluthochdruck_pl",
        ordered=True,
    )
    out["med_diabetes"] = combine_first_and_make_categorical(
        merged,
        "med_diabetes_pequiv",
        "med_diabetes_pl",
        ordered=True,
    )
    out["med_krebs"] = combine_first_and_make_categorical(
        merged,
        "med_krebs_pequiv",
        "med_krebs_pl",
        ordered=True,
    )
    out["med_herzkrankheit"] = combine_first_and_make_categorical(
        merged,
        "med_herzkrankheit_pequiv",
        "med_herzkrankheit_pl",
        ordered=True,
    )
    out["med_schlaganfall"] = combine_first_and_make_categorical(
        merged,
        "med_schlaganfall_pequiv",
        "med_schlaganfall_pl",
        ordered=True,
    )
    out["med_gelenk"] = combine_first_and_make_categorical(
        merged,
        "med_gelenk_pequiv",
        "med_gelenk_pl",
        ordered=True,
    )
    out["med_gewicht"] = merged["med_gewicht_pequiv"].combine_first(
        merged["med_gewicht_pl"]
    )
    out["med_groesse"] = merged["med_groesse_pequiv"].combine_first(
        merged["med_groesse_pl"]
    )
    out["bmi"] = merged["bmi_pequiv"].combine_first(merged["bmi_pl"])
    out["bmi_dummy"] = merged["bmi_dummy_pequiv"].combine_first(merged["bmi_dummy_pl"])
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
        pl: DataFrame containing the cleaned pl data.
        pkal: DataFrame containing the cleaned pkal data.

    Returns:
        DataFrame containing the merged received transfer variables.
    """
    out = pd.DataFrame(index=pl.index)
    merged = pd.merge(pl, pkal, on=["hh_id", "survey_year"], how="outer")
    out[["p_id", "hh_id", "survey_year"]] = pl[["p_id", "hh_id", "survey_year"]].copy()
    out["mutterschaftsgeld_bezug"] = combine_first_and_make_categorical(
        merged,
        "mutterschaftsgeld_bezug_pl",
        "mutterschaftsgeld_bezug_pkal",
        ordered=False,
    )
    return out
