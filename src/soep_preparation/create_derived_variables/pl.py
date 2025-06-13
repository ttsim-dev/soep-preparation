"""Functions to create variables for pre-processed pl data."""

import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_lowest_float_dtype,
    apply_lowest_int_dtype,
    create_dummy,
)


def _private_rentenversichung_beitrag(raw_data: pd.DataFrame) -> pd.Series:
    private_rente_beitrag_m = pd.Series(
        raw_data["private_rente_beitrag_m_2013"].where(
            raw_data["survey_year"] != 2018,  # noqa: PLR2004
            raw_data["private_rente_beitrag_m_2018"],
        ),
        name="private_rente_beitrag_m",
    )

    data = pd.concat([raw_data["p_id"], private_rente_beitrag_m], axis=1)
    out = data.groupby("p_id")["private_rente_beitrag_m"].ffill()
    return apply_lowest_float_dtype(out)


def create_derived_variables(data: pd.DataFrame) -> pd.DataFrame:
    """Create derived variables for the pl data.

    Args:
        data: The required data.

    Returns:
        The derived variables.
    """
    med_vars = [
        "med_pl_schwierigkeit_treppen",
        "med_pl_schwierigkeit_taten",
        "med_pl_schlaf",
        "med_pl_diabetes",
        "med_pl_asthma",
        "med_pl_herzkrankheit",
        "med_pl_krebs",
        "med_pl_schlaganfall",
        "med_pl_migraene",
        "med_pl_bluthochdruck",
        "med_pl_depressiv",
        "med_pl_demenz",
        "med_pl_gelenk",
        "med_pl_ruecken",
        "med_pl_sonst",
        "med_pl_raucher",
    ]

    out = pd.DataFrame(index=data.index)

    out["private_rente_beitrag_m"] = _private_rentenversichung_beitrag(
        data[
            [
                "p_id",
                "survey_year",
                "private_rente_beitrag_m_2013",
                "private_rente_beitrag_m_2018",
            ]
        ],
    )
    out["med_pl_schwierigkeit_treppen_dummy"] = create_dummy(
        data["med_pl_schwierigkeit_treppen"], [1, 2], "isin"
    )
    out["bmi_pl"] = data["med_pl_gewicht"] / ((data["med_pl_groesse"] / 100) ** 2)
    out["bmi_pl_dummy"] = apply_lowest_int_dtype(out["bmi_pl"] >= 30)  # noqa: PLR2004
    out["med_pl_subjective_status_dummy"] = create_dummy(
        data["med_pl_subjective_status"], 3, "geq"
    )
    med_var_data = pd.concat(
        [data[med_vars], out["med_pl_subjective_status_dummy"]],
        axis=1,
    )
    out["frailty_pl"] = apply_lowest_float_dtype(med_var_data.mean(axis=1))
    return out
