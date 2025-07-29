"""Example task to create merge variables to a dataset."""

from typing import Annotated

import pandas as pd
from pytask import task

from soep_preparation.config import DATA_CATALOGS, SURVEY_YEARS
from soep_preparation.dataset_merging.helper import create_dataset
from soep_preparation.utilities.error_handling import fail_if_input_has_invalid_type

VARIABLES = [
    "hh_id",
    "p_id",
    "survey_year",
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


@task(after="task_create_metadata_mapping")
def task_merge_variables(
    variables: Annotated[list[str], VARIABLES],
) -> Annotated[pd.DataFrame, DATA_CATALOGS["merged"]["example_merged_dataset"]]:
    """Example task merging based on variable names to create dataset.

    Args:
        variables: A list of variable names to be used for merging.

    Returns:
        The variables merged into a dataset.

    Raises:
        TypeError: If input mapping or variables is not of expected type.
    """
    fail_if_input_has_invalid_type(input_=variables, expected_dtypes=["list"])
    return create_dataset(
        variables=variables,
        min_survey_year=min(SURVEY_YEARS),
        max_survey_year=max(SURVEY_YEARS),
        survey_years=None,
    )
