"""Functions to create variables for pre-processed pequiv data."""

import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_smallest_float_dtype,
    apply_smallest_int_dtype,
)


def create_derived_variables(data: pd.DataFrame) -> pd.DataFrame:
    """Create derived variables for the pequiv data.

    Args:
        data: The required data.

    Returns:
        The derived variables.
    """
    med_vars = [
        "med_schwierigkeiten_anziehen_pequiv",
        "med_schwierigkeiten_bett",
        "med_schwierigkeiten_einkauf",
        "med_schwierigkeiten_hausarb",
        "med_schwierigkeiten_treppen_pequiv",
        "med_krankenhaus_pequiv",
        "med_bluthochdruck_pequiv",
        "med_diabetes_pequiv",
        "med_krebs_pequiv",
        "med_herzkrankheit_pequiv",
        "med_schlaganfall_pequiv",
        "med_gelenk_pequiv",
        "med_psych_pequiv",
    ]

    out = pd.DataFrame(index=data.index)

    out["bmi_pequiv"] = apply_smallest_float_dtype(
        data["med_gewicht_pequiv"] / ((data["med_größe_pequiv"] / 100) ** 2),
    )
    out["bmi_dummy_pequiv"] = apply_smallest_int_dtype(
        out["bmi_pequiv"] >= 30,  # noqa: PLR2004
    )
    out["med_subjective_status_dummy_pequiv"] = apply_smallest_int_dtype(
        data["med_subjective_status_pequiv"] <= 5,  # noqa: PLR2004
    )
    med_var_data = pd.concat(
        [
            data[med_vars],
            out[["bmi_dummy_pequiv", "med_subjective_status_dummy_pequiv"]],
        ],
        axis=1,
    )
    out["frailty_pequiv"] = apply_smallest_float_dtype(med_var_data.mean(axis=1))

    # hh social benefits yearly to average monthly amounts
    out["alg2_hh_betrag_m_pequiv"] = apply_smallest_float_dtype(
        data["alg2_hh_betrag_y_pequiv"] / 12
    )
    out["kindergeld_hh_betrag_m_pequiv"] = apply_smallest_float_dtype(
        data["kindergeld_hh_betrag_y_pequiv"] / 12
    )
    out["kinderzuschlag_hh_betrag_m_pequiv"] = apply_smallest_float_dtype(
        data["kinderzuschlag_hh_betrag_y_pequiv"] / 12
    )
    out["betreuungsgeld_hh_betrag_m"] = apply_smallest_float_dtype(
        data["betreuungsgeld_hh_betrag_y"] / 12
    )
    out["wohngeld_hh_betrag_m_pequiv"] = apply_smallest_float_dtype(
        data["wohngeld_hh_betrag_y_pequiv"] / 12
    )
    return out
