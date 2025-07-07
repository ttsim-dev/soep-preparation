"""Functions to create pre-processed pgen data."""

import re

import pandas as pd

from soep_preparation.utilities.data_manipulator import create_dummy


def _in_education(
    employment: "pd.Series[pd.Categorical]",
    occupation: "pd.Series[pd.Categorical]",
) -> "pd.Series[pd.Categorical]":
    in_education = [
        "Aspiranten (1990 Ost)",
        "Auszubildende (1984-1999), Lehrlinge (1990 Ost)",
        "Auszubildende, gewerblich-technisch (ab 2000)",
        "Auszubildende, kaufmännisch (ab 2000)",
        "NE: in Ausbildung, inkl. Weiterbildung, Berufsausbildung, Lehre",
        "Volontäre, Praktikanten",
    ]
    out = create_dummy(employment, "Ausbildung, Lehre")
    # Set in_education to missing if out of the labor force -- could mean anything.
    out = out.where(employment != "Nicht erwerbstätig", pd.NA)
    return out.fillna(create_dummy(occupation, in_education, "isin"))


def _self_employed_occupations(
    occupation: "pd.Series[pd.Categorical]",
) -> list:
    """Occupation names that indicate self employment."""
    occupation_names = list(occupation.dropna().unique())
    occupations_of_interest = re.compile("^.*Freiberufler.*$|^.*selbstä.*$")
    return list(filter(occupations_of_interest.match, occupation_names))


def _education(
    casmin: "pd.Series[pd.Categorical]",
    isced: "pd.Series[pd.Categorical]",
) -> "pd.Series[pd.Categorical]":
    """Transform education variable to three levels."""
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


def create_derived_variables(data: pd.DataFrame) -> pd.DataFrame:
    """Create derived variables for the pgen data.

    Args:
        data: The required cleaned data.

    Returns:
        Derived variables.
    """
    out = pd.DataFrame(index=data.index)
    out["german"] = create_dummy(data["first_nationality"], "Deutschland")
    out["retired"] = create_dummy(data["occupation_status"], "NE: Rentner/Rentnerin")
    out["in_education"] = _in_education(
        data["employment_status"],
        data["occupation_status"],
    )
    out["self_employed"] = create_dummy(
        data["occupation_status"],
        _self_employed_occupations(data["occupation_status"]),
        "isin",
    )
    out["military"] = create_dummy(
        data["occupation_status"],
        "NE: Wehr- und Zivildienst",
    )

    out["erwerbstätig_y"] = (
        create_dummy(data["employment_status"], "Nicht erwerbstätig", "neq")
    ) & (~out["in_education"])

    out["nicht_erwerbstätig_y"] = create_dummy(
        data["employment_status"],
        "Nicht erwerbstätig",
    )
    out["unemployed_y"] = create_dummy(
        data["occupation_status"],
        "NE: arbeitslos gemeldet",
    )
    out["ft_y"] = create_dummy(data["employment_status"], "Voll erwerbstätig")
    out["pt_employed_y"] = create_dummy(
        data["employment_status"], "Teilzeitbeschäftigung"
    )
    out["geringfügig_erwbstätig"] = create_dummy(
        data["employment_status"],
        "Unregelmässig, geringfügig erwerbstät.",
    )
    out["werkstatt"] = create_dummy(
        data["employment_status"],
        "Werkstatt für behinderte Menschen (seit 1998)",
    )
    out["beamte"] = create_dummy(
        series=data["occupation_status"],
        value_for_comparison="Beamte",
        comparison_type="startswith",
    )
    out["parental_leave"] = create_dummy(
        data["laborforce_status"],
        "NE: Mutterschutz/Elternzeit (seit 1991)",
    )
    out["highest_education"] = _education(
        data["education_casmin"],
        data["education_isced"],
    )
    return out
