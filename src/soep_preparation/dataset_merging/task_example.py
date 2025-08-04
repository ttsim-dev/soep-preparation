"""Example task to create merge variables to a dataset."""

from typing import Annotated, Any

import pandas as pd

from soep_preparation.config import DATA_CATALOGS, ROOT, SURVEY_YEARS
from soep_preparation.dataset_merging.helper import create_dataset_from_variables
from soep_preparation.utilities.error_handling import fail_if_input_has_invalid_type

VARIABLES = [
    "hh_id",
    "p_id",
    "survey_year",
    "net_income_hh_m",
    "east_germany",
    "age",
    "disabled",
    "vertragliche_arbeitszeit_w",  # use tatsächliche_arbeitszeit_w?
    "number_of_children",
    "child_number",
    "p_id_child",
    "birth_year_child",
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
