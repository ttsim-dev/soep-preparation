"""Combine variables from the modules pequiv and pl."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    combined_categorical,
    convert_to_categorical,
    create_dummy,
)


def combine(pequiv: pd.DataFrame, pl: pd.DataFrame) -> pd.DataFrame:
    """Combine variables from the cleaned pequiv and pl modules.

    Args:
        pequiv: Cleaned pequiv module.
        pl: Cleaned pl module.

    Returns:
        Combined pequiv and pl modules. If contents conflict with each other,
             the one from pequiv takes precedence.
    """
    merged = pd.merge(
        left=pequiv,
        right=pl,
        on=["p_id", "hh_id", "survey_year"],
        how="outer",
    )
    out = pd.DataFrame(index=merged.index)
    out["p_id"] = merged["p_id"]
    out["hh_id"] = merged["hh_id"]
    out["hh_id_original"] = merged["hh_id_original"]
    out["survey_year"] = merged["survey_year"]

    # pequiv records this as a bool; pl as a 3-level intensity. Match pequiv's
    # binary granularity so the combined output is single-typed.
    pl_treppen_dummy = convert_to_categorical(
        series=create_dummy(
            series=merged["med_difficulty_stairs_pl"],
            value_for_comparison=["A little", "A lot"],
            comparison_type="isin",
        ),
        ordered=True,
    )
    out["med_difficulty_stairs"] = combined_categorical(
        series_1=merged["med_difficulty_stairs_pequiv"],
        series_2=pl_treppen_dummy,
        ordered=True,
    )
    out["med_hypertension"] = combined_categorical(
        series_1=merged["med_hypertension_pequiv"],
        series_2=merged["med_hypertension_pl"],
        ordered=True,
    )
    out["med_diabetes"] = combined_categorical(
        series_1=merged["med_diabetes_pequiv"],
        series_2=merged["med_diabetes_pl"],
        ordered=True,
    )
    out["med_cancer"] = combined_categorical(
        series_1=merged["med_cancer_pequiv"],
        series_2=merged["med_cancer_pl"],
        ordered=True,
    )
    out["med_heart_disease"] = combined_categorical(
        series_1=merged["med_heart_disease_pequiv"],
        series_2=merged["med_heart_disease_pl"],
        ordered=True,
    )
    out["med_stroke"] = combined_categorical(
        series_1=merged["med_stroke_pequiv"],
        series_2=merged["med_stroke_pl"],
        ordered=True,
    )
    out["med_joint_disease"] = combined_categorical(
        series_1=merged["med_joint_disease_pequiv"],
        series_2=merged["med_joint_disease_pl"],
        ordered=True,
    )
    out["med_weight"] = merged["med_weight_pequiv"].combine_first(
        merged["med_weight_pl"]
    )
    out["med_height"] = merged["med_height_pequiv"].combine_first(
        merged["med_height_pl"]
    )
    out["bmi"] = merged["bmi_pequiv"].combine_first(merged["bmi_pl"])
    out["obese"] = merged["obese_pequiv"].combine_first(merged["obese_pl"])
    out["med_subjective_status"] = combined_categorical(
        series_1=merged["med_subjective_status_pequiv"],
        series_2=merged["med_subjective_status_pl"],
        ordered=True,
    )
    out["frailty"] = merged["frailty_pequiv"].combine_first(merged["frailty_pl"])

    out["kindesunterhalt_received_m"] = merged[
        "kindesunterhalt_received_m_pequiv"
    ].combine_first(merged["kindesunterhalt_received_m_pl"])

    return out
