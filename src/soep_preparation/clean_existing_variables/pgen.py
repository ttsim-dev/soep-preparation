"""Functions to pre-process variables for a raw pgen dataset."""

import pandas as pd

from soep_preparation.clean_existing_variables import month_mapping
from soep_preparation.utilities import (
    apply_lowest_int_dtype,
    object_to_bool_categorical,
    object_to_float,
    object_to_int_categorical,
    object_to_str_categorical,
)


def _weekly_working_hours_actual(
    actual: "pd.Series[pd.Categorical]",
    employment_status: "pd.Series[pd.Categorical]",
) -> "pd.Series[float]":
    out = object_to_float(actual)
    return out.where(employment_status != "Nicht erwerbstätig", 0)


def _weekly_working_hours_contract(
    contract: "pd.Series[pd.Categorical]",
    employment_status: "pd.Series[pd.Categorical]",
) -> "pd.Series[float]":
    out = object_to_float(contract)
    return out.where(employment_status != "Nicht erwerbstätig", 0)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the pgen dataset."""
    out = pd.DataFrame()

    out["hh_id_orig"] = apply_lowest_int_dtype(raw_data["cid"])
    out["hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])
    out["survey_year"] = apply_lowest_int_dtype(raw_data["syear"])
    # there are multiple categories for ['Kurdistan', 'Malaysia', 'Montenegro']
    # they have been reduced into one category each
    out["nationality_first"] = object_to_str_categorical(
        raw_data["pgnation"],
    )
    out["status_refugee"] = object_to_str_categorical(raw_data["pgstatus_refu"])
    out["marital_status"] = object_to_str_categorical(raw_data["pgfamstd"])
    out["curr_earnings_m"] = object_to_float(raw_data["pglabgro"]).fillna(0)
    out["net_wage_m"] = object_to_float(raw_data["pglabnet"]).fillna(0)
    out["occupation_status"] = object_to_str_categorical(raw_data["pgstib"])
    out["employment_status"] = object_to_str_categorical(
        raw_data["pgemplst"],
    )
    out["laborf_status"] = object_to_str_categorical(raw_data["pglfs"])
    out["dauer_im_betrieb"] = object_to_float(raw_data["pgerwzeit"])
    out["weekly_working_hours_actual"] = _weekly_working_hours_actual(
        raw_data["pgtatzeit"],
        out["employment_status"],
    )
    out["weekly_working_hours_contract"] = _weekly_working_hours_contract(
        raw_data["pgvebzeit"],
        out["employment_status"],
    )
    out["public_service"] = object_to_bool_categorical(
        raw_data["pgoeffd"],
        renaming={"[2] nein": False, "[1] ja": True},
        ordered=True,
    )
    out["size_company_raw"] = object_to_str_categorical(
        raw_data["pgbetr"].replace(
            {-5: "[-5] in Fragebogenversion nicht enthalten"},
        ),
    )
    out["size_company"] = object_to_str_categorical(raw_data["pgallbet"])
    out["pgen_grund_beschäftigungsende"] = object_to_str_categorical(
        raw_data["pgjobend"],
    )
    out["exp_full_time"] = object_to_float(raw_data["pgexpft"])
    out["exp_part_time"] = object_to_float(raw_data["pgexppt"])
    out["exp_unempl"] = object_to_float(raw_data["pgexpue"])

    out["education_isced_old"] = object_to_str_categorical(raw_data["pgisced97"])
    out["education_isced"] = object_to_str_categorical(
        raw_data["pgisced11"],
    )
    out["education_isced_cat"] = apply_lowest_int_dtype(
        out["education_isced"].cat.codes,
    )
    out["education_casmin"] = object_to_str_categorical(
        raw_data["pgcasmin"],
        nr_identifiers=2,
    )
    out["education_casmin_cat"] = apply_lowest_int_dtype(
        out["education_casmin"].cat.codes,
    )
    out["month_interview"] = object_to_int_categorical(
        raw_data["pgmonth"],
        renaming=month_mapping.de,
    )
    return out
