import pandas as pd

from soep_preparation.utilities import apply_lowest_float_dtype, apply_lowest_int_dtype


def manipulate(data: pd.DataFrame) -> pd.DataFrame:
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
        "med_pe_subj_status",
    ]
    out = data[data.columns[~data.columns.isin([med_vars])]]
    out[med_vars] = data.groupby("p_id")[med_vars].ffill()
    out["bmi_pe"] = apply_lowest_float_dtype(
        out["med_pe_gewicht"] / ((out["med_pe_groesse"] / 100) ** 2),
    )
    out["bmi_pe_dummy"] = apply_lowest_int_dtype(out["bmi_pe"] >= 30)
    out["med_pe_subj_status_dummy"] = apply_lowest_int_dtype(
        data["med_pe_subj_status"] <= 5,
    )

    med_vars.append("bmi_pe_dummy")
    med_vars.append("med_pe_subj_status_dummy")
    med_vars.remove("med_pe_subj_status")

    out["frailty_pe"] = apply_lowest_float_dtype(out[med_vars].mean(axis=1))
    return out
