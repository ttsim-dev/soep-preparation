"""Functions to pre-process variables for a raw pgen dataset.

Functions:
- clean: Coordinates the pre-processing for the dataset.

Usage:
    Import this module and call clean to pre-process variables.
"""

import pandas as pd

from soep_preparation.clean_existing_variables import month_mapping
from soep_preparation.utilities import (
    apply_lowest_int_dtype,
    bool_categorical,
    float_categorical_to_float,
    int_to_int_categorical,
    str_categorical,
    str_categorical_to_int_categorical,
)


def _weekly_working_hours_actual(
    actual: "pd.Series[pd.Categorical]",
    employment_status: "pd.Series[pd.Categorical]",
) -> "pd.Series[float]":
    out = float_categorical_to_float(actual)
    return out.where(employment_status != "Nicht erwerbstätig", 0)


def _weekly_working_hours_contract(
    contract: "pd.Series[pd.Categorical]",
    employment_status: "pd.Series[pd.Categorical]",
) -> "pd.Series[float]":
    out = float_categorical_to_float(contract)
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
    out["nationality_first"] = str_categorical(
        raw_data["pgnation"],
        reduce=True,
    )
    out["german"] = out["nationality_first"].dropna() == "Deutschland"
    out["status_refugee"] = str_categorical(raw_data["pgstatus_refu"])
    out["marital_status"] = str_categorical(raw_data["pgfamstd"])
    out["curr_earnings_m"] = float_categorical_to_float(raw_data["pglabgro"]).fillna(0)
    out["net_wage_m"] = float_categorical_to_float(raw_data["pglabnet"]).fillna(0)
    out["occupation_status"] = str_categorical(raw_data["pgstib"])
    out["employment_status"] = str_categorical(
        raw_data["pgemplst"],
    )
    out["laborf_status"] = str_categorical(raw_data["pglfs"])
    out["dauer_im_betrieb"] = float_categorical_to_float(raw_data["pgerwzeit"])
    out["weekly_working_hours_actual"] = _weekly_working_hours_actual(
        raw_data["pgtatzeit"],
        out["employment_status"],
    )
    out["weekly_working_hours_contract"] = _weekly_working_hours_contract(
        raw_data["pgvebzeit"],
        out["employment_status"],
    )
    out["public_service"] = bool_categorical(
        raw_data["pgoeffd"],
        renaming={"[2] nein": False, "[1] ja": True},
        ordered=True,
    )
    out["size_company_raw"] = str_categorical(
        raw_data["pgbetr"].cat.rename_categories(
            {-5: "[-5] in Fragebogenversion nicht enthalten"},
        ),
    )
    out["size_company"] = str_categorical(raw_data["pgallbet"])
    out["pgen_grund_beschäftigungsende"] = str_categorical(raw_data["pgjobend"])
    out["exp_full_time"] = float_categorical_to_float(raw_data["pgexpft"])
    out["exp_part_time"] = float_categorical_to_float(raw_data["pgexppt"])
    out["exp_unempl"] = float_categorical_to_float(raw_data["pgexpue"])
    out["education_isced_old"] = str_categorical(raw_data["pgisced97"])
    out["education_isced"] = str_categorical(
        raw_data["pgisced11"],
        ordered=True,
    )
    out["education_isced_cat"] = int_to_int_categorical(
        out["education_isced"].cat.codes,
        ordered=True,
    )
    out["education_casmin"] = str_categorical(
        raw_data["pgcasmin"],
        nr_identifiers=2,
        ordered=True,
    )
    out["education_casmin_cat"] = int_to_int_categorical(
        out["education_casmin"].cat.codes,
        ordered=True,
    )
    out["month_interview"] = str_categorical_to_int_categorical(
        raw_data["pgmonth"],
        renaming=month_mapping.de,
    )
    return out
