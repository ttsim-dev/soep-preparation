"""Example task to create merge variables to a dataset."""

from typing import Annotated, Any

import pandas as pd

from soep_preparation.config import DATA_CATALOGS, ROOT, SURVEY_YEARS
from soep_preparation.dataset_merging.helper import create_dataset_from_variables
from soep_preparation.utilities.error_handling import fail_if_input_has_invalid_type

VARIABLES = [
    "age",
    "vertragliche_arbeitszeit_w_current",
    "disability_degree",
    "east_germany",
    "self_employed",
    "gross_labor_income_previous_month_m",
    "gender",
    # for comparison with GETTSIM calculations only
    "net_income_m_hh",
    "net_labor_income_previous_month_m",
    "number_of_children",
    "child_number",
    "p_id_child",
    "birth_year_child",
    "bezog_wohngeld_hh",
    "bezog_arbeitslosengeld_2_hh",
    "bezog_kinderzuschlag_hh",
    "employment_status",
    "labor_force_status",
    "occupation_status",
    # wage equation
    "full_time_working_experience",
    "part_time_working_experience",
    "unemployment_experience",
    "tenure",
    "highest_education",
    "foreigner",
    "federal_state_of_residence",
    "partnership_status",
    "einkommen_aus_zinsen_dividenden_m_hh",
    "einkommen_aus_vermietung_verpachtung_m_hh",
]


def task_merge_variables(
    mapping_variable_to_data_file: Annotated[dict, DATA_CATALOGS["metadata"]["merged"]],
    variables: Annotated[list[str], VARIABLES],
) -> Annotated[pd.DataFrame, DATA_CATALOGS["merged"]["example_merged_dataset"]]:
    """Example task merging based on variable names to create dataset.

    Args:
        mapping_variable_to_data_file: A mapping of variable names to dataset names.
        variables: A list of variable names to be used for merging.

    Returns:
        The variables merged into a dataset.

    Raises:
        TypeError: If input mapping or variables is not of expected type.
    """
    _error_handling_task(mapping=mapping_variable_to_data_file, variables=variables)
    return create_dataset_from_variables(
        variables=variables,
        min_and_max_survey_years=(min(SURVEY_YEARS), max(SURVEY_YEARS)),
        mapping_variable_to_data_file=mapping_variable_to_data_file,
    )


def task_copy_to_root(
    example_merged_dataset: Annotated[
        pd.DataFrame, DATA_CATALOGS["merged"]["example_merged_dataset"]
    ],
) -> None:
    """Copy the example merged dataset to the root directory.

    Args:
        example_merged_dataset: The merged dataset to be copied.
    """
    example_merged_dataset.to_pickle(ROOT / "example_merged_dataset.pkl")


def _error_handling_task(mapping: Any, variables: Any) -> None:  # noqa: ANN401
    fail_if_input_has_invalid_type(input_=mapping, expected_dtypes=["dict"])
    fail_if_input_has_invalid_type(input_=variables, expected_dtypes=["list"])
