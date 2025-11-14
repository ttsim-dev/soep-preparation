"""Example task to create merge variables to a dataset."""

from pathlib import Path
from typing import Annotated, Any

import pandas as pd
import pytask
import yaml

from soep_preparation.config import DATA_CATALOGS, SRC, SURVEY_YEARS
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


@pytask.mark.skip()
def task_merge_variables(
    map_path: Annotated[
        Path, SRC / "create_metadata" / "variable_to_metadata_mapping.yaml"
    ],
    variables: Annotated[list[str], VARIABLES],
) -> Annotated[pd.DataFrame, DATA_CATALOGS["merged"]["example_merged_dataset"]]:
    """Example task merging based on variable names to create dataset.

    Args:
        map_path: Path to the variable to module mapping.
        variables: A list of variable names to be used for merging.

    Returns:
        The variables merged into a dataset.

    Raises:
        TypeError: If input mapping or variables is not of expected type.
    """
    _error_handling_task(map_path=map_path, variables=variables)
    with Path.open(map_path, "r", encoding="utf-8") as file:
        map_variable_to_module = yaml.safe_load(file)
    return create_dataset_from_variables(
        variables=variables,
        min_and_max_survey_years=(min(SURVEY_YEARS), max(SURVEY_YEARS)),
        map_variable_to_data_file=map_variable_to_module,
    )


def _error_handling_task(map_path: Any, variables: Any) -> None:  # noqa: ANN401
    fail_if_input_has_invalid_type(input_=map_path, expected_dtypes=["pathlib.Path"])
    fail_if_input_has_invalid_type(input_=variables, expected_dtypes=["list"])
