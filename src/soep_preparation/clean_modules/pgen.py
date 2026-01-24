"""Clean and convert SOEP pgen variables to appropriate data types."""

import re

import pandas as pd

from soep_preparation.utilities import month_mapping
from soep_preparation.utilities.data_manipulator import (
    apply_smallest_int_dtype,
    create_dummy,
    object_to_bool_categorical,
    object_to_float,
    object_to_int_categorical,
    object_to_str_categorical,
)


def _education(
    casmin: pd.Series[pd.Categorical],
    isced: pd.Series[pd.Categorical],
) -> pd.Series[pd.Categorical]:
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
        ).astype("string[pyarrow]"),
        ordered=True,
    )
    return out.astype(cat_type)


def _in_education(
    employment: pd.Series[pd.Categorical],
    occupation: pd.Series[pd.Categorical],
) -> pd.Series[pd.Categorical]:
    in_education = [
        "Aspiranten (1990 Ost)",
        "Auszubildende (1984-1999), Lehrlinge (1990 Ost)",
        "Auszubildende, gewerblich-technisch (ab 2000)",
        "Auszubildende, kaufmännisch (ab 2000)",
        "NE: in Ausbildung, inkl. Weiterbildung, Berufsausbildung, Lehre",
        "Volontäre, Praktikanten",
    ]
    out = create_dummy(series=employment, value_for_comparison="Ausbildung, Lehre")
    # Set in_education to missing if out of the labor force -- could mean anything.
    out = out.where(employment != "Nicht erwerbstätig", pd.NA)
    return out.fillna(
        create_dummy(
            series=occupation, value_for_comparison=in_education, comparison_type="isin"
        )
    )


def _self_employed_occupations(
    occupation: pd.Series[pd.Categorical],
) -> list:
    """Occupation names that indicate self employment."""
    occupation_names = list(occupation.dropna().unique())
    occupations_of_interest = re.compile("^.*Freiberufler.*$|^.*selb(st)?stä.*$")
    return [
        name
        for name in occupation_names
        if occupations_of_interest.match(name) is not None
    ]


def _weekly_working_hours_fill_non_working(
    working_hours: pd.Series,
    employment_status: pd.Series,
) -> pd.Series:
    out = object_to_float(working_hours)
    return out.where(employment_status != "Nicht erwerbstätig", 0)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned variables from the pgen module.

    Args:
        raw_data: The raw pgen data.

    Returns:
        The processed pgen data.
    """
    out = pd.DataFrame()

    out["hh_id_original"] = apply_smallest_int_dtype(raw_data["cid"])
    out["hh_id"] = apply_smallest_int_dtype(raw_data["hid"])
    out["p_id"] = apply_smallest_int_dtype(raw_data["pid"])
    out["survey_year"] = apply_smallest_int_dtype(raw_data["syear"])

    out["month_interview"] = object_to_int_categorical(
        series=raw_data["imonth"],
        renaming=month_mapping.de,
        ordered=True,
    )
    out["education_isced_97"] = object_to_str_categorical(
        raw_data["pgisced97"],
        renaming={
            "general elemantary": "general elementary",  # codespell:ignore elemantary
            "higher education": "higher education",
            "higher vocational": "higher vocational",
            "in school": "in school",
            "inadequately": "inadequately",
            "middle vocational": "middle vocational",
            "vocational + Abi": "vocational + Abi",
        },
    )
    out["education_isced"] = object_to_str_categorical(raw_data["pgisced11"])
    out["education_isced_cat"] = apply_smallest_int_dtype(
        out["education_isced"].cat.codes
    )
    out["education_casmin"] = object_to_str_categorical(
        series=raw_data["pgcasmin"],
        nr_identifiers=2,
    )
    out["education_casmin_cat"] = apply_smallest_int_dtype(
        out["education_casmin"].cat.codes
    )
    out["highest_education"] = _education(
        casmin=out["education_casmin"],
        isced=out["education_isced"],
    )

    # individual current status
    # there are multiple categories for ['Kurdistan', 'Malaysia', 'Montenegro']
    # they have been reduced into one category each
    out["first_nationality"] = object_to_str_categorical(raw_data["pgnation"])
    out["german"] = create_dummy(
        series=out["first_nationality"], value_for_comparison="Deutschland"
    )
    out["refugee_status"] = object_to_str_categorical(raw_data["pgstatus_refu"])
    out["marital_status"] = object_to_str_categorical(raw_data["pgfamstd"])
    out["labor_force_status"] = object_to_str_categorical(raw_data["pglfs"])
    out["occupation_status"] = object_to_str_categorical(raw_data["pgstib"])
    out["employment_status"] = object_to_str_categorical(raw_data["pgemplst"])
    out["total_full_time_working_experience"] = object_to_float(raw_data["pgexpft"])
    out["total_part_time_working_experience"] = object_to_float(raw_data["pgexppt"])
    out["total_unemployment_experience"] = object_to_float(raw_data["pgexpue"])
    out["tenure"] = object_to_float(raw_data["pgerwzeit"])
    out["retired"] = create_dummy(
        series=out["occupation_status"], value_for_comparison="NE: Rentner/Rentnerin"
    )
    out["in_education"] = _in_education(
        employment=out["employment_status"],
        occupation=out["occupation_status"],
    )
    out["self_employed"] = create_dummy(
        series=out["occupation_status"],
        value_for_comparison=_self_employed_occupations(out["occupation_status"]),
        comparison_type="isin",
    )
    out["military"] = create_dummy(
        series=out["occupation_status"],
        value_for_comparison="NE: Wehr- und Zivildienst",
    )

    out["erwerbstätig"] = (
        create_dummy(
            series=out["employment_status"],
            value_for_comparison="Nicht erwerbstätig",
            comparison_type="neq",
        )
    ) & (~out["in_education"])

    out["nicht_erwerbstätig"] = create_dummy(
        series=out["employment_status"],
        value_for_comparison="Nicht erwerbstätig",
    )
    out["arbeitslos_gemeldet"] = create_dummy(
        series=out["occupation_status"],
        value_for_comparison="NE: arbeitslos gemeldet",
    )
    out["voll_erwerbstätig"] = create_dummy(
        series=out["employment_status"], value_for_comparison="Voll erwerbstätig"
    )
    out["in_teilzeit_erwerbstätig"] = create_dummy(
        series=out["employment_status"], value_for_comparison="Teilzeitbeschäftigung"
    )
    out["unregelmäßig_oder_geringfügig_erwerbstätig"] = create_dummy(
        series=out["employment_status"],
        value_for_comparison="Unregelmässig, geringfügig erwerbstät.",
    )
    out["werkstatt_für_behinderte_menschen"] = create_dummy(
        series=out["employment_status"],
        value_for_comparison="Werkstatt für behinderte Menschen (seit 1998)",
    )
    out["beamter"] = create_dummy(
        series=out["occupation_status"],
        value_for_comparison="Beamte",
        comparison_type="startswith",
    )
    out["mutterschutz_elternzeit"] = create_dummy(
        series=out["labor_force_status"],
        value_for_comparison="NE: Mutterschutz/Elternzeit (seit 1991)",
    )

    # individual work information
    out["gross_labor_income_previous_month_m"] = object_to_float(
        raw_data["pglabgro"]
    ).fillna(0)
    out["net_labor_income_previous_month_m"] = object_to_float(
        raw_data["pglabnet"]
    ).fillna(0)
    out["tatsächliche_arbeitszeit_w"] = _weekly_working_hours_fill_non_working(
        working_hours=raw_data["pgtatzeit"],
        employment_status=out["employment_status"],
    )
    out["vertragliche_arbeitszeit_w"] = _weekly_working_hours_fill_non_working(
        working_hours=raw_data["pgvebzeit"],
        employment_status=out["employment_status"],
    )
    out["im_öffentlichen_dienst"] = object_to_bool_categorical(
        series=raw_data["pgoeffd"],
        renaming={"[2] nein": False, "[1] ja": True},
        ordered=True,
    )
    out["betriebsgröße"] = object_to_str_categorical(raw_data["pgallbet"])
    out["betriebsgröße_detailliert_aber_inkonsistente_kategorien"] = (
        object_to_str_categorical(
            raw_data["pgbetr"].replace(
                {-5: "[-5] in Fragebogenversion nicht enthalten"},
            ),
        )
    )
    out["grund_beschäftigungsende"] = object_to_str_categorical(
        raw_data["pgjobend"],
    )

    return out
