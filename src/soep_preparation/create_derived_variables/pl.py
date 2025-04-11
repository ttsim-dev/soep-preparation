"""Functions to create datasets for a pre-processed pl dataset."""

import pandas as pd

from soep_preparation.utilities import (
    apply_lowest_float_dtype,
    apply_lowest_int_dtype,
)


def _priv_rentenv_beitr(raw_data: pd.DataFrame) -> pd.Series:
    priv_rentenv_beitr_m = pd.Series(
        raw_data["priv_rente_beitr_2013_m"].where(
            raw_data["survey_year"] != 2018,  # noqa: PLR2004
            raw_data["priv_rente_beitr_2018_m"],
        ),
        name="priv_rentenv_beitr_m",
    )

    data = pd.concat([raw_data["p_id"], priv_rentenv_beitr_m], axis=1)
    out = data.groupby("p_id")["priv_rentenv_beitr_m"].ffill()
    return apply_lowest_float_dtype(out)


def create_derived_variables(data: pd.DataFrame) -> pd.DataFrame:
    """Create derived variables for the pl dataset.

    Args:
        data (pd.DataFrame): The dataset required.

    Returns:
        pd.DataFrame: The dataset of derived variables.
    """
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
    ]

    out = pd.DataFrame(index=data.index)

    out["priv_rentenv_beitr_m"] = _priv_rentenv_beitr(
        data[
            [
                "p_id",
                "survey_year",
                "priv_rente_beitr_2013_m",
                "priv_rente_beitr_2018_m",
            ]
        ],
    )
    out["bmi_pl"] = data["med_pl_gewicht"] / ((data["med_pl_groesse"] / 100) ** 2)
    out["bmi_pl_dummy"] = apply_lowest_int_dtype(out["bmi_pl"] >= 30)  # noqa: PLR2004
    out["med_pl_subj_status_numerical_dummy"] = apply_lowest_int_dtype(
        data["med_pl_subj_status"] >= 5,  # noqa: PLR2004
    )
    med_var_data = pd.concat(
        [data[med_vars], out["med_pl_subj_status_numerical_dummy"]],
        axis=1,
    )
    out["frailty_pl"] = apply_lowest_float_dtype(med_var_data.mean(axis=1))
    return out
