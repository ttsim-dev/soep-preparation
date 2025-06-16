"""Functions to create variables for pre-processed pl data."""

import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_smallest_float_dtype,
    apply_smallest_int_dtype,
    create_dummy,
)


def _private_rente_beitrag_monatlich(raw_data: pd.DataFrame) -> pd.Series:
    private_rente_beitrag_m = pd.Series(
        raw_data["private_rente_beitrag_m_2013"].where(
            raw_data["survey_year"] != 2018,  # noqa: PLR2004
            raw_data["private_rente_beitrag_m_2018"],
        ),
        name="private_rente_beitrag_m",
    )

    data = pd.concat([raw_data["p_id"], private_rente_beitrag_m], axis=1)
    out = data.groupby("p_id")["private_rente_beitrag_m"].ffill()
    return apply_smallest_float_dtype(out)


def create_derived_variables(data: pd.DataFrame) -> pd.DataFrame:
    """Create derived variables for the pl data.

    Args:
        data: The required data.

    Returns:
        The derived variables.
    """
    med_vars = [
        "med_schwierigkeit_treppen_pl",
        "med_schwierigkeit_taten_pl",
        "med_schlaf_pl",
        "med_diabetes_pl",
        "med_asthma_pl",
        "med_herzkrankheit_pl",
        "med_krebs_pl",
        "med_schlaganfall_pl",
        "med_migraene_pl",
        "med_bluthochdruck_pl",
        "med_depressiv_pl",
        "med_demenz_pl",
        "med_gelenk_pl",
        "med_ruecken_pl",
        "med_sonst_pl",
        "med_raucher_pl",
    ]

    out = pd.DataFrame(index=data.index)

    out["private_rente_beitrag_m"] = _private_rente_beitrag_monatlich(
        data[
            [
                "p_id",
                "survey_year",
                "private_rente_beitrag_m_2013",
                "private_rente_beitrag_m_2018",
            ]
        ],
    )
    out["med_schwierigkeit_treppen_dummy_pl"] = create_dummy(
        data["med_schwierigkeit_treppen_pl"], [1, 2], "isin"
    )
    out["bmi_pl"] = data["med_gewicht_pl"] / ((data["med_groesse_pl"] / 100) ** 2)
    out["bmi_dummy_pl"] = apply_smallest_int_dtype(out["bmi_pl"] >= 30)  # noqa: PLR2004
    out["med_subjective_status_dummy_pl"] = create_dummy(
        data["med_subjective_status_pl"], 3, "geq"
    )
    med_var_data = pd.concat(
        [data[med_vars], out["med_subjective_status_dummy_pl"]],
        axis=1,
    )
    out["frailty_pl"] = apply_smallest_float_dtype(med_var_data.mean(axis=1))
    return out
