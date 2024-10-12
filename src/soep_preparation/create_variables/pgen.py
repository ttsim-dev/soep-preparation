import re

from soep_preparation.config import pd
from soep_preparation.utilities import create_dummy


def _in_education(
    employment: "pd.Series[pd.Categorical]",
    occupation: "pd.Series[pd.Categorical]",
) -> "pd.Series[pd.Categorical]":
    in_education = [
        "in Ausbildung, inkl. Weiterbildung, Berufsausbildung, Lehre",
        "Auszubildende (bis 1999)",
        "Auszubildende, gewerblich-technisch",
        "Auszubildende, gewerblich-technisch (ab 2000)",
        "Auszubildende, kaufmaennisch",
        "Volontäre, Praktikanten",
        "Aspiranten",
        "NE: in Ausbildung, inkl. Weiterbildung, Berufsausbildung, Lehre",
    ]
    out = create_dummy(employment, "Ausbildung, Lehre")
    # Set in_education to missing if out of the labor force -- could mean anything.
    out = out.where(employment != "Nicht erwerbstätig", pd.NA)
    return out.fillna(create_dummy(occupation, in_education, "isin"))


def _self_employed_occupations(
    occupation: "pd.Series[pd.Categorical]",
) -> list:
    occ_names = list(occupation.dropna().unique())
    occs_of_interest = re.compile("^.*Freiberufler.*$|^.*selbstä.*$")
    return list(filter(occs_of_interest.match, occ_names))


def _education(
    casmin: "pd.Series[pd.Categorical]",
    isced: "pd.Series[pd.Categorical]",
) -> "pd.Series[pd.Categorical]":
    out = casmin.combine_first(isced).map(
        {
            "in school": "PRIMARY_AND_LOWER_SECONDARY",
            "inadequately completed": "PRIMARY_AND_LOWER_SECONDARY",
            "general elementary school": "PRIMARY_AND_LOWER_SECONDARY",
            "basic vocational qualification": "PRIMARY_AND_LOWER_SECONDARY",
            "intermediate vocational": "UPPER_SECONDARY",
            "intermediate general qualification": "UPPER_SECONDARY",
            "general maturity certificate": "UPPER_SECONDARY",
            "vocational maturity certificate": "UPPER_SECONDARY",
            "lower tertiary education": "TERTIARY",
            "higher tertiary education": "TERTIARY",
            "Primary education": "PRIMARY_AND_LOWER_SECONDARY",
            "Lower secondary education": "PRIMARY_AND_LOWER_SECONDARY",
            "Upper secondary education": "UPPER_SECONDARY",
            "Post-secondary non-tertiary education": "UPPER_SECONDARY",
            "Short-cycle tertiary education": "UPPER_SECONDARY",
            "Bachelor s or equivalent level": "TERTIARY",
            "Master s or equivalent level": "TERTIARY",
            "Doctoral or equivalent level": "TERTIARY",
        },
    )
    cat_type = pd.CategoricalDtype(
        categories=pd.Series(
            ["PRIMARY_AND_LOWER_SECONDARY", "UPPER_SECONDARY", "TERTIARY"],
        ).astype("str[pyarrow]"),
        ordered=True,
    )
    return out.astype(cat_type)


def manipulate(data: pd.DataFrame) -> pd.DataFrame:
    out = data.copy()
    out["german"] = create_dummy(out["nationality_first"], "Deutschland")
    out["retired"] = create_dummy(out["occupation_status"], "NE: Rentner/Rentnerin")
    out["in_education"] = _in_education(
        out["employment_status"],
        out["occupation_status"],
    )
    out["self_employed"] = create_dummy(
        out["occupation_status"],
        _self_employed_occupations(out["occupation_status"]),
        "isin",
    )
    out["military"] = create_dummy(
        out["occupation_status"],
        "NE: Wehr- und Zivildienst",
    )
    out["erwerbstätig"] = (
        create_dummy(out["employment_status"], "Nicht erwerbstätig", "neq")
    ) & (~out["in_education"])

    out["nicht_erwerbstätig"] = create_dummy(
        out["employment_status"],
        "Nicht erwerbstätig",
    )
    out["unemployed"] = create_dummy(
        out["occupation_status"],
        "NE: arbeitslos gemeldet",
    )
    out["full_time"] = create_dummy(out["employment_status"], "Voll erwerbstätig")
    out["part_time"] = create_dummy(out["employment_status"], "Teilzeitbeschäftigung")
    out["geringfügig_erwb"] = create_dummy(
        out["employment_status"],
        "Unregelmässig, geringfügig erwerbstät.",
    )
    out["werkstatt"] = create_dummy(
        out["employment_status"],
        "Werkstatt für behinderte Menschen",
    )
    out["beamte"] = out["occupation_status"].str.startswith("Beamte", na=False)
    out["parental_leave"] = create_dummy(
        out["laborf_status"],
        "NE: Mutterschutz/Elternzeit (seit 1991) ",
    )
    out["education"] = _education(
        out["education_casmin"],
        out["education_isced"],
    )
    return out
