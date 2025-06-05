"""Functions to create variables for pre-processed pequiv data."""

import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_lowest_float_dtype,
    apply_lowest_int_dtype,
)


def create_derived_variables(data: pd.DataFrame) -> pd.DataFrame:
    """Create derived variables for the pequiv data.

    Args:
        data: The required data.

    Returns:
        The derived variables.
    """
    med_vars = [
        "med_pequiv_schwierigkeiten_anziehen",
        "med_schwierigkeiten_bett",
        "med_schwierigkeiten_einkauf",
        "med_schwierigkeiten_hausarb",
        "med_pequiv_schwierigkeiten_treppen",
        "med_pequiv_krnkhaus",
        "med_pequiv_bluthochdruck",
        "med_pequiv_diabetes",
        "med_pequiv_krebs",
        "med_pequiv_herzkrankheit",
        "med_pequiv_schlaganfall",
        "med_pequiv_gelenk",
        "med_pequiv_psych",
    ]

    out = pd.DataFrame(index=data.index)

    out["bmi_pequiv"] = apply_lowest_float_dtype(
        data["med_pequiv_gewicht"] / ((data["med_pequiv_groesse"] / 100) ** 2),
    )
    out["bmi_pequiv_dummy"] = apply_lowest_int_dtype(
        out["bmi_pequiv"] >= 30,  # noqa: PLR2004
    )
    out["med_pequiv_subjective_status_dummy"] = apply_lowest_int_dtype(
        data["med_pequiv_subjective_status"] <= 5,  # noqa: PLR2004
    )
    med_var_data = pd.concat(
        [
            data[med_vars],
            out[["bmi_pequiv_dummy", "med_pequiv_subjective_status_dummy"]],
        ],
        axis=1,
    )
    out["frailty_pequiv"] = apply_lowest_float_dtype(med_var_data.mean(axis=1))

    # hh social benefits yearly to average monthly amounts
    out["alg2_pequiv_hh_monatlicher_betrag"] = apply_lowest_float_dtype(
        data["alg2_pequiv_hh_jaehrlicher_betrag"] / 12
    )
    out["kindergeld_pequiv_hh_monatlicher_betrag"] = apply_lowest_float_dtype(
        data["kindergeld_pequiv_hh_jaehrlicher_betrag"] / 12
    )
    out["kinderzuschlag_pequiv_hh_monatlicher_betrag"] = apply_lowest_float_dtype(
        data["kinderzuschlag_pequiv_hh_jaehrlicher_betrag"] / 12
    )
    out["childcare_subsidy_hh_monthly_amount"] = apply_lowest_float_dtype(
        data["childcare_subsidy_hh_annual_amount"] / 12
    )
    out["wohngeld_pequiv_hh_monatlicher_betrag"] = apply_lowest_float_dtype(
        data["wohngeld_pequiv_hh_jaehrlicher_betrag"] / 12
    )
    return out
