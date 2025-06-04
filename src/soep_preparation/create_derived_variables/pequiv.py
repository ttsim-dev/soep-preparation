"""Functions to create variables for pre-processed pequiv data."""
import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_lowest_float_dtype,
    apply_lowest_int_dtype,
)


def create_derived_variables(data: pd.DataFrame) -> pd.DataFrame:
    """Create derived variables for the pequiv data.

    Args:
        data (pd.DataFrame): The required data.

    Returns:
        pd.DataFrame: The derived variables.
    """
    med_vars = [
        "med_pe_schw_anziehen",
        "med_pe_schw_bett",
        "med_pe_schw_einkauf",
        "med_pe_schw_hausarb",
        "med_pe_schw_treppen",
        "med_pe_krnkhaus",
        "med_pe_bluthdrck",
        "med_pe_diabetes",
        "med_pe_krebs",
        "med_pe_herzkr",
        "med_pe_schlaganf",
        "med_pe_gelenk",
        "med_pe_psych",
    ]

    out = pd.DataFrame(index=data.index)

    out["bmi_pe"] = apply_lowest_float_dtype(
        data["med_pe_gewicht"] / ((data["med_pe_groesse"] / 100) ** 2),
    )
    out["bmi_pe_dummy"] = apply_lowest_int_dtype(out["bmi_pe"] >= 30)  # noqa: PLR2004
    out["med_pe_subj_status_dummy"] = apply_lowest_int_dtype(
        data["med_pe_subj_status"] <= 5,  # noqa: PLR2004
    )
    med_var_data = pd.concat(
        [data[med_vars], out[["bmi_pe_dummy", "med_pe_subj_status_dummy"]]],
        axis=1,
    )
    out["frailty_pequiv"] = apply_lowest_float_dtype(med_var_data.mean(axis=1))
    return out
