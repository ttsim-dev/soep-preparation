"""Clean and convert SOEP pgen variables to appropriate data types."""

import pandas as pd

from soep_preparation.clean_existing_variables import month_mapping
from soep_preparation.utilities.series_manipulator import (
    apply_smallest_int_dtype,
    object_to_bool_categorical,
    object_to_float,
    object_to_int_categorical,
    object_to_str_categorical,
)


def _weekly_working_hours_fill_non_working(
    working_hours: pd.Series,
    employment_status: pd.Series,
) -> pd.Series:
    out = object_to_float(working_hours)
    return out.where(employment_status != "Nicht erwerbstätig", 0)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the pgen data file.

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
        raw_data["pgmonth"],
        renaming=month_mapping.de,
        ordered=True,
    )
    out["education_isced_old"] = object_to_str_categorical(raw_data["pgisced97"])
    out["education_isced"] = object_to_str_categorical(raw_data["pgisced11"])
    out["education_isced_cat"] = apply_smallest_int_dtype(
        out["education_isced"].cat.codes
    )
    out["education_casmin"] = object_to_str_categorical(
        raw_data["pgcasmin"],
        nr_identifiers=2,
    )
    out["education_casmin_cat"] = apply_smallest_int_dtype(
        out["education_casmin"].cat.codes
    )

    # individual current status
    # there are multiple categories for ['Kurdistan', 'Malaysia', 'Montenegro']
    # they have been reduced into one category each
    out["first_nationality"] = object_to_str_categorical(raw_data["pgnation"])
    out["refugee_status"] = object_to_str_categorical(raw_data["pgstatus_refu"])
    out["marital_status"] = object_to_str_categorical(raw_data["pgfamstd"])
    out["laborforce_status"] = object_to_str_categorical(raw_data["pglfs"])
    out["occupation_status"] = object_to_str_categorical(raw_data["pgstib"])
    out["employment_status"] = object_to_str_categorical(raw_data["pgemplst"])
    out["total_full_time_working_experience"] = object_to_float(raw_data["pgexpft"])
    out["total_part_time_working_experience"] = object_to_float(raw_data["pgexppt"])
    out["total_unemployment_experience"] = object_to_float(raw_data["pgexpue"])
    out["tenure"] = object_to_float(raw_data["pgerwzeit"])

    # individual work information
    out["gross_labor_income_previous_month"] = object_to_float(
        raw_data["pglabgro"]
    ).fillna(0)
    out["net_labor_income_previous_month"] = object_to_float(
        raw_data["pglabnet"]
    ).fillna(0)
    out["weekly_working_hours_actual"] = _weekly_working_hours_fill_non_working(
        raw_data["pgtatzeit"],
        out["employment_status"],
    )
    out["weekly_working_hours_contract"] = _weekly_working_hours_fill_non_working(
        raw_data["pgvebzeit"],
        out["employment_status"],
    )
    out["public_service"] = object_to_bool_categorical(
        raw_data["pgoeffd"],
        renaming={"[2] nein": False, "[1] ja": True},
        ordered=True,
    )
    out["size_company"] = object_to_str_categorical(raw_data["pgallbet"])
    out["size_company_granular"] = object_to_str_categorical(
        raw_data["pgbetr"].replace(
            {-5: "[-5] in Fragebogenversion nicht enthalten"},
        ),
    )
    out["grund_beschäftigungsende"] = object_to_str_categorical(
        raw_data["pgjobend"],
    )

    return out
