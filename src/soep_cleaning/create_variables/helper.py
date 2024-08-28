import re

import numpy as np
from soep_cleaning.config import pd
from soep_cleaning.utilities import apply_lowest_float_dtype, apply_lowest_int_dtype


def pgen_manipulation(df: pd.DataFrame) -> pd.DataFrame:  # task
    # clean logically
    # TODO: refactor this code to adhere to the functional approach
    df.loc[
        df["employment_status"] == "Nicht erwerbstätig",
        ["weekly_working_hours_actual", "weekly_working_hours_contract"],
    ] = 0

    # new variables
    df["german"] = df["nationality_first"].dropna() == "Deutschland"

    # Work status
    df = create_dummies_for_occupation_and_employment_status(df)

    # Generate qualification variables from casmin and isced education variables
    # use casmin in education_mapping to create education
    df["education"] = df["education_casmin"].dropna().map(education_mapping.casmin)
    # fill missing values from isced in education_mapping
    df.loc[df["education"].isnull(), "education"] = (
        df["education_isced"].dropna().map(education_mapping.isced)
    )
    val_quali = [
        "primary_and_lower_secondary",
        "upper_secondary",
        "tertiary",
    ]
    cat_type = pd.CategoricalDtype(categories=val_quali, ordered=True)
    df["education"] = df["education"].astype(cat_type)
    return df


def create_dummies_for_occupation_and_employment_status(out):
    out["retired"] = out["occupation_status"].dropna() == "NE: Rentner/Rentnerin"
    out["in_education"] = out["employment_status"].dropna() == "Ausbildung, Lehre"
    in_education = [
        "in Ausbildung, inkl. Weiterbildung, Berufsausbildung, Lehre",
        "in Ausbildung,             inkl. Weiterbildung, Berufsausbildung, Lehre",
        "Auszubildende (bis 1999)",
        "Auszubildende, gewerblich-technisch",
        "Auszubildende, gewerblich-technisch (ab 2000)",
        "Auszubildende, kaufmaennisch",
        "Volontäre, Praktikanten",
        "Aspiranten",
        "NE: in Ausbildung, inkl. Weiterbildung, Berufsausbildung, Lehre",
    ]
    # Don't use employment status to determine in ausbildung if not working
    out.loc[out["employment_status"] == "Nicht erwerbstätig", "in_education"] = np.nan
    out["in_education"] = out["in_education"].fillna(
        out["occupation_status"].dropna().isin(in_education),
    )
    list_of_occ_names = list(out["occupation_status"].dropna().unique())
    occs_of_interest = re.compile("^.*Freiberufler.*$|^.*selbstä.*$")
    self_employed = list(filter(occs_of_interest.match, list_of_occ_names))
    out["self_employed"] = out["occupation_status"].dropna().isin(self_employed)
    out["military"] = out["occupation_status"].dropna() == "NE: Wehr- und Zivildienst"

    out["erwerbstätig"] = (
        out["employment_status"].dropna() != "Nicht erwerbstätig"
    ) & (~out["in_education"].dropna())
    out["nicht_erwerbstätig"] = (
        out["employment_status"].dropna() == "Nicht erwerbstätig"
    )
    out["unemployed"] = out["occupation_status"].dropna() == "NE: arbeitslos gemeldet"
    out["full_time"] = out["employment_status"].dropna() == "Voll erwerbstätig"
    out["part_time"] = out["employment_status"].dropna() == "Teilzeitbeschäftigung"
    out["geringfügig_erwb"] = (
        out["employment_status"].dropna() == "Unregelmässig, geringfügig erwerbstät."
    )

    out["werkstatt"] = (
        out["employment_status"].dropna() == "Werkstatt für behinderte Menschen"
    )
    out["beamte"] = out["occupation_status"].str.startswith("Beamte", na=False)
    out["parental_leave"] = (
        out["laborf_status"].dropna() == "NE: Mutterschutz/Elternzeit (seit 1991) "
    )
    return out


def pequiv_manipulation(df: pd.DataFrame) -> pd.DataFrame:  # task
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
    out = df[~med_vars]
    out[med_vars] = df.groupby("p_id")[med_vars].ffill()
    out["bmi_pe"] = df["med_pe_gewicht"] / ((df["med_pe_groesse"] / 100) ** 2)
    out["bmi_pe_dummy"] = apply_lowest_int_dtype(out["bmi_pe"] >= 30)
    out["med_pe_subj_status_dummy"] = apply_lowest_int_dtype(
        df["med_pe_subj_status"] <= 5,
    )

    med_vars.append("bmi_pe_dummy")
    med_vars.append("med_pe_subj_status_dummy")
    med_vars.remove("med_pe_subj_status")

    out["frailty_pe"] = apply_lowest_float_dtype(df[med_vars].mean(axis=1))
    return out
