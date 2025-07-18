"""Example task to create merge variables to a dataset."""

from typing import Annotated, Any

import pandas as pd
import pytask

from soep_preparation.config import DATA_CATALOGS, ROOT, SURVEY_YEARS
from soep_preparation.dataset_merging.helper import create_dataset_from_variables
from soep_preparation.utilities.error_handling import fail_if_input_has_invalid_type

VARIABLES = [
    "age",
    "weekly_working_hours_actual",
    "disability_degree",
    "birth_year",
    "east_germany",
    "self_employed",
    "einkünfte_aus_selbstständiger_arbeit_betrag_m",
    "rental_income_amount_m",
    "einkünfte_aus_arbeit_betrag_m",
    "income_from_forest_and_agriculture",
    "capital_income_amount_m",
    "income_from_other_sources",
    "gesetzliche_rente_empfangener_betrag_m",
    "private_rente_beitrag_m",
    "children_care_facility_costs_m_current",
    "person_that_pays_childcare_expenses",
    "joint_taxation",
    "private_altersvorsorge_betrag_m",
    "private_zusatzkrankenversicherung_betrag_m",
    "has_children",
    "single_parent",
    "is_child",
    "pointer_partner",
    "p_id_father",
    "p_id_mother",
    "in_education",
    "id_recipient_child_allowance",
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


def task_store_example_merged_dataset(
    example_merged_dataset: Annotated[
        pd.DataFrame, DATA_CATALOGS["merged"]["example_merged_dataset"]
    ],
) -> None:
    """Task to store the example merged dataset.

    Args:
        example_merged_dataset: The merged dataset to be stored.
    """
    unique_individuals = example_merged_dataset.drop_duplicates(
        subset="p_id", keep="first"
    )
    out = unique_individuals[unique_individuals["hh_id"].notna()].reset_index(drop=True)
    out.to_parquet(
        ROOT / "dataset.pkl",
        engine="pyarrow",
        index=False,
    )


def _error_handling_task(mapping: Any, variables: Any) -> None:  # noqa: ANN401
    fail_if_input_has_invalid_type(input_=mapping, expected_dtypes=["dict"])
    fail_if_input_has_invalid_type(input_=variables, expected_dtypes=["list"])
