import re

import numpy as np
from soep_cleaning.config import pd


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
