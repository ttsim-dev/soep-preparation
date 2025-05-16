"""Example task to create a merged panel dataset."""

from typing import Annotated

import pandas as pd

from soep_preparation.config import DATA_CATALOGS, SURVEY_YEARS
from soep_preparation.dataset_merging.helper import create_panel_dataset
from src.soep_preparation.utilities import fail_if_invalid_input

COLUMNS = [
    "age",
    "birth_month",
    "bmi_pe",
    "hh_gewicht_nur_neue",
    "hh_position",
    "hh_strat",
    "n_kids_total",
    "p_id_father",
]


def task_merge_columns(
    columns_to_dataset_mapping: Annotated[dict, DATA_CATALOGS["merged"]["metadata"]],
    columns: Annotated[list[str], COLUMNS],
) -> Annotated[pd.DataFrame, DATA_CATALOGS["merged"]["example_merged_dataset"]]:
    """Example task to merge datasets based on column names.

    Args:
        columns_to_dataset_mapping (dict): A mapping of column names to dataset names.
        columns (list[str]): A list of column names to be used for merging.

    Returns:
        pd.DataFrame: The merged dataset.
    """
    _error_handling_task(columns_to_dataset_mapping, columns)
    return create_panel_dataset(
        columns_to_dataset_mapping=columns_to_dataset_mapping,
        columns=columns,
        min_and_max_survey_years=(min(SURVEY_YEARS), max(SURVEY_YEARS)),
    )


def _error_handling_task(mapping, columns):
    fail_if_invalid_input(mapping, "dict")
    fail_if_invalid_input(columns, "list")
