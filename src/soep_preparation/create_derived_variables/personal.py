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
    out["birth_month"] = merged["birth_month_from_ppathl"].combine_first(
        merged["birth_month_from_bioedu"]
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
        "med_pequiv_schwierigkeiten_treppen",
        "med_pl_schwierigkeit_treppen_dummy",
        ordered=True,
    )
    out["med_bluthochdruck"] = combine_first_and_make_categorical(
        merged,
        "med_pequiv_bluthochdruck",
        "med_pl_bluthochdruck",
        ordered=True,
    )
    out["med_diabetes"] = combine_first_and_make_categorical(
        merged,
        "med_pequiv_diabetes",
        "med_pl_diabetes",
        ordered=True,
    )
    out["med_krebs"] = combine_first_and_make_categorical(
        merged,
        "med_pequiv_krebs",
        "med_pl_krebs",
        ordered=True,
    )
    out["med_herzkrankheit"] = combine_first_and_make_categorical(
        merged,
        "med_pequiv_herzkrankheit",
        "med_pl_herzkrankheit",
        ordered=True,
    )
    out["med_schlaganfall"] = combine_first_and_make_categorical(
        merged,
        "med_pequiv_schlaganfall",
        "med_pl_schlaganfall",
        ordered=True,
    )
    out["med_gelenk"] = combine_first_and_make_categorical(
        merged,
        "med_pequiv_gelenk",
        "med_pl_gelenk",
        ordered=True,
    )
    out["med_gewicht"] = merged["med_pequiv_gewicht"].combine_first(
        merged["med_pl_gewicht"]
    )
    out["med_groesse"] = merged["med_pequiv_groesse"].combine_first(
        merged["med_pl_groesse"]
    )
    out["bmi"] = merged["bmi_pequiv"].combine_first(merged["bmi_pl"])
    out["bmi_dummy"] = merged["bmi_pequiv_dummy"].combine_first(merged["bmi_pl_dummy"])
    out["med_subjective_status"] = merged["med_pequiv_subjective_status"].combine_first(
        merged["med_pl_subjective_status"]
    )
    out["med_subjective_status_dummy"] = merged[
        "med_pequiv_subjective_status_dummy"
    ].combine_first(merged["med_pl_subjective_status_dummy"])
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
