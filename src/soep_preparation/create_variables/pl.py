from soep_preparation.config import pd
from soep_preparation.utilities import (
    apply_lowest_int_dtype,
)


def _priv_rentenv_beitr(
    beitr_2013_m: pd.Series,
    beitr_2018_m: pd.Series,
    year: pd.Series,
) -> pd.Series:
    out = beitr_2013_m
    return out.where(year != 2018, beitr_2018_m)


def manipulate(data: pd.DataFrame) -> pd.DataFrame:
    out = data.copy()
    out["priv_rentenv_beitr_m"] = _priv_rentenv_beitr(
        out["prv_rente_beitr_2013_m"],
        out["prv_rente_beitr_2018_m"],
        out["year"],
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
    out["bmi_pl_dummy"] = apply_lowest_int_dtype(out["bmi_pl"] >= 30)
    out["med_pl_subj_status__numerical_dummy"] = apply_lowest_int_dtype(
        out["med_pl_subj_status"] >= 3,
    )
    med_vars.append("med_pl_subj_status__numerical_dummy")
    med_vars.remove("med_pl_subj_status")
    out["frailty_pl"] = out[med_vars].mean(axis=1)
    return out
