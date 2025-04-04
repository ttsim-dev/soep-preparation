"""Functions to create variables for a pre-processed pl dataset.

Functions:
- manipulate: Coordinates the variable creation process for the dataset.

Usage:
    Import this module and call manipulate to generate new variables.
"""

import pandas as pd

from soep_preparation.utilities import (
    apply_lowest_int_dtype,
)


def _priv_rentenv_beitr(data: pd.DataFrame) -> pd.Series:
    other_survey_year = 2018
    data["priv_rentenv_beitr_m"] = data["priv_rente_beitr_2013_m"].where(
        data["survey_year"] != other_survey_year,
        data["priv_rente_beitr_2018_m"],
    )
    data["priv_rentenv_beitr_m"] = data.groupby("p_id")["priv_rentenv_beitr_m"].ffill()
    return data["priv_rentenv_beitr_m"]


def create_derived_variables(data: pd.DataFrame) -> pd.DataFrame:
    """Create derived variables for the pl dataset.

    Args:
        data (pd.DataFrame): The dataset required.

    Returns:
        pd.DataFrame: The dataset of derived variables.
    """
    out = data.copy()
    out["priv_rentenv_beitr_m"] = _priv_rentenv_beitr(
        out[
            [
                "p_id",
                "survey_year",
                "priv_rente_beitr_2013_m",
                "priv_rente_beitr_2018_m",
            ]
        ],
    )
    med_vars = [
        "med_pl_schw_treppen",
        "med_pl_schw_taten",
        "med_pl_schlaf",
        "med_pl_diabetes",
        "med_pl_asthma",
        "med_pl_herzkr",
        "med_pl_krebs",
        "med_pl_schlaganf",
        "med_pl_migraene",
        "med_pl_bluthdrck",
        "med_pl_depressiv",
        "med_pl_demenz",
        "med_pl_gelenk",
        "med_pl_ruecken",
        "med_pl_sonst",
        "med_pl_raucher",
        "med_pl_subj_status",
    ]
    out[med_vars] = out[med_vars].apply(apply_lowest_int_dtype, axis=0)
    out[med_vars] = out.groupby("p_id")[med_vars].ffill()
    out["bmi_pl"] = out["med_pl_gewicht"] / ((out["med_pl_groesse"] / 100) ** 2)
    out["bmi_pl_dummy"] = apply_lowest_int_dtype(out["bmi_pl"] >= 30)  # noqa: PLR2004
    out["med_pl_subj_status_numerical_dummy"] = apply_lowest_int_dtype(
        out["med_pl_subj_status"] >= 5,  # noqa: PLR2004
    )
    med_vars.append("med_pl_subj_status_numerical_dummy")
    med_vars.remove("med_pl_subj_status")
    out["frailty_pl"] = out[med_vars].mean(axis=1)
    return out
