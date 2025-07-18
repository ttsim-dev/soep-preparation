"""Example task to create merge variables to a dataset."""

from typing import Annotated, Any

import pandas as pd
import pytask

from soep_preparation.config import DATA_CATALOGS, SURVEY_YEARS
from soep_preparation.dataset_merging.helper import create_dataset_from_variables
from soep_preparation.utilities.error_handling import fail_if_input_has_invalid_type

VARIABLES = [
    "age",
    "birth_month",
    "bmi",
    "hh_weighting_factor_new_only",
    "relationship_to_head_of_hh",
    "hh_strat",
    "number_of_children",
    "p_id_father",
    "frailty",
]


@pytask.mark.try_last
def task_merge_variables(
    variable_to_data_file_mapping: Annotated[dict, DATA_CATALOGS["metadata"]["merged"]],
    variables: Annotated[list[str], VARIABLES],
) -> Annotated[pd.DataFrame, DATA_CATALOGS["merged"]["example_merged_dataset"]]:
    """Example task merging based on variable names to create dataset.

    Args:
        variable_to_data_file_mapping: A mapping of variable names to dataset names.
        variables: A list of variable names to be used for merging.

    Returns:
        The variables merged into a dataset.

    Raises:
        TypeError: If input mapping or variables is not of expected type.
    """
    _error_handling_task(mapping=variable_to_data_file_mapping, variables=variables)
    return create_dataset_from_variables(
        variables=variables,
        min_and_max_survey_years=(min(SURVEY_YEARS), max(SURVEY_YEARS)),
        variable_to_data_file_mapping=variable_to_data_file_mapping,
    )


def _error_handling_task(mapping: Any, variables: Any) -> None:  # noqa: ANN401
    fail_if_input_has_invalid_type(input_=mapping, expected_dtypes=["dict"])
    fail_if_input_has_invalid_type(input_=variables, expected_dtypes=["list"])
