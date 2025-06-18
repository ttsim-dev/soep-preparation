from typing import Annotated

import pandas as pd
from gettsim import config as gettsim_config

from soep_preparation.config import DATA_CATALOGS
from soep_preparation.gettsim_preparation.rente import renteneintritt_jahr
from soep_preparation.utilities import create_dummy, object_to_int_categorical


def _renaming_vars_for_gettsim(data):
    out = data.copy()
    return out.rename(
        columns={
            "child": "kind",
            "gross_wage_m": "bruttolohn_m",
            "age": "alter",
            "female": "weiblich",
            "retired": "rentner",
            "east_germany": "wohnort_ost",
            "in_education": "in_ausbildung",
            "self_employed": "selbstständig",
            "self_empl_earnings_m": "eink_selbst_m",
            "rental_income_m": "eink_vermietung_m",
            "capital_income_m": "kapitaleink_brutto_m",
            "heating_costs_m_hh": "heizkosten_m_hh",
            "living_space_hh": "wohnfläche_hh",
            "working_hours_w": "arbeitsstunden_w",
            "birthday": "geburtstag",
            "birth_month": "geburtsmonat",
            "birth_year": "geburtsjahr",
            "retirement_entry_year": "jahr_renteneintr",
            "disability_degree": "behinderungsgrad",
            "building_year_hh": "immobilie_baujahr_hh",
            "wealth_hh": "vermoegen_beduerft_hh",
        },
    )


def _create_variables_for_gettsim(data):
    out = data.copy()
    out["retirement_entry_year"] = object_to_int_categorical(
        renteneintritt_jahr(
            out[
                [
                    "p_id",
                    "retired",
                    "survey_year",
                    "birth_year",
                    "birth_month",
                    "exp_full_time",
                    "exp_part_time",
                ]
            ],
        ),
    )
    out["female"] = create_dummy(out["gender"], "female")
    return out


def task_prepare_soep_data_for_gettsim(
    data: Annotated[pd.DataFrame, DATA_CATALOGS["merged"]["long_dataset"]],
) -> Annotated[pd.DataFrame, DATA_CATALOGS["merged"]["prepared_soep_data"]]:
    """Prepare SOEP data for gettsim.

    Args:
        data: The merged dataset with imputations and additions.

    Returns:
        The prepared dataset for gettsim.
    """
    out = data.reset_index()
    out_wide = _create_variables_for_gettsim(out)

    out_cols = set(out_renamed.columns)
    gettsim_cols = set(gettsim_config.TYPES_INPUT_VARIABLES.keys())
    gettsim_cols_in_out = gettsim_cols.intersection(out_cols)
    gettsim_cols_not_in_out = gettsim_cols.difference(out_cols)

    out_renamed = _renaming_vars_for_gettsim(out_wide)
    # gettsim_synthetic().columns
    # out.astype(gettsim_config.TYPES_INPUT_VARIABLES)
    return pd.DataFrame()
