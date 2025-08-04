"""Example task to create merge variables to a dataset."""

from typing import Annotated, Any

import pandas as pd

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


def task_merge_variables(
    map_variable_to_data_file: Annotated[dict, DATA_CATALOGS["metadata"]["mapping"]],
    variables: Annotated[list[str], VARIABLES],
) -> Annotated[pd.DataFrame, DATA_CATALOGS["merged"]["example_merged_dataset"]]:
    """Example task merging based on variable names to create dataset.

    Args:
        map_variable_to_data_file: A mapping of variable names to dataset names.
        variables: A list of variable names to be used for merging.

    Returns:
        The variables merged into a dataset.

    Raises:
        TypeError: If input mapping or variables is not of expected type.
    """
    _error_handling_task(mapping=map_variable_to_data_file, variables=variables)
    return create_dataset_from_variables(
        variables=variables,
        min_and_max_survey_years=(min(SURVEY_YEARS), max(SURVEY_YEARS)),
        map_variable_to_data_file=map_variable_to_data_file,
    )


def _error_handling_task(mapping: Any, variables: Any) -> None:  # noqa: ANN401
    fail_if_input_has_invalid_type(input_=mapping, expected_dtypes=["dict"])
    fail_if_input_has_invalid_type(input_=variables, expected_dtypes=["list"])
