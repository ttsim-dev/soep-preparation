"""Helper function to create final dataset."""

from difflib import get_close_matches
from typing import Literal

import pandas as pd

from soep_preparation.config import METADATA, POTENTIAL_INDEX_VARIABLES
from soep_preparation.utilities.error_handling import (
    fail_if_empty,
    fail_if_input_has_invalid_type,
)


def create_final_dataset(
    modules: dict[str, pd.DataFrame],
    variables: list[str],
    survey_years: list[int],
) -> pd.DataFrame:
    """Merge variables for specified survey years into final dataset.

    A list of variables and timeframe needs to be specified to create a dataset.
    Variables are results of the pipeline of cleaning and combining variables.

    Args:
        modules: All modules required to create the final dataset.
        variables: A list of variable names for the merged dataset to contain.
        survey_years: Survey years to be included in the dataset.

    Returns:
        The dataset with specified variables and survey years.

    Raises:
        TypeError: If the input types are not as expected.
        ValueError: If variables or variable mapping are empty or
        contain faulty variables.
        Or if survey years out of survey range.
        Or if inadequate merging behavior.

    Notes:
        `modules` contains the cleaned and combined modules with the
         variables of interest.
        `variables` contains the variables
        created, renamed, and derived from the raw SOEP data files
        that will be part of the merged dataset.
        `survey_years` are those years the final dataset will contain variables for.
        To receive data for just one year (e.g. `2025`) specify `survey_years=[2025]`.

    Examples:
        For an example see `task_example.py`.
    """
    _error_handling(
        modules=modules,
        variables=variables,
        survey_years=survey_years,
    )
    harmonized_variables = _harmonize_variables(variables)

    dataset_merging_information = _get_sorted_dataset_merging_information(
        modules=modules,
        variables=harmonized_variables,
        survey_years=survey_years,
    )

    return _merge_data(
        merging_information=dataset_merging_information,
    )


def _error_handling(
    modules: dict[str, pd.DataFrame],
    variables: list[str],
    survey_years: list[int],
) -> None:
    fail_if_input_has_invalid_type(input_=variables, expected_dtypes=["list"])
    fail_if_empty(variables, name="variables")

    # TODO (@hmgaudecker): we need at-least one module with the variable  # noqa: TD003
    # `survey_year` to check for valid survey years
    valid_survey_years = modules["pl"]["survey_year"].unique().tolist()
    _fail_if_survey_years_not_valid(
        survey_years=survey_years,
        valid_survey_years=valid_survey_years,
    )
    _fail_if_invalid_variable(variables=variables)


def _fail_if_invalid_variable(variables: list[str]) -> None:
    for variable in variables:
        if variable not in METADATA and variable not in POTENTIAL_INDEX_VARIABLES:
            closest_matches = get_close_matches(
                variable,
                METADATA.keys(),
                n=3,
                cutoff=0.6,
            )
            matches = "    \n".join(
                f"{m}: {METADATA[m]['module']}" for m in closest_matches
            )
            msg = (
                "Variable {variable} is not present in the modules you provided.\n"
                "The closest matches and corresponding modules are:\n\n"
                f"    {matches}\n"
            )
            raise ValueError(msg)


def _fail_if_survey_years_not_valid(
    survey_years: list[int],
    valid_survey_years: list[int],
) -> None:
    if not all(year in valid_survey_years for year in survey_years):
        msg = f"""Expected survey years to be in {valid_survey_years},
        got {survey_years} instead."""
        raise ValueError(msg)


def _sort_dataset_merging_information(
    dataset_merging_information: dict[str, dict],
) -> dict[str, dict]:
    return dict(
        sorted(
            dataset_merging_information.items(),
            key=lambda item: len(item[1]["index_variables"]),
            reverse=True,
        ),
    )


def _harmonize_variables(
    variables: list[str],
) -> list[str]:
    return [v for v in variables if v not in POTENTIAL_INDEX_VARIABLES]


def _get_sorted_dataset_merging_information(
    modules: dict[str, pd.DataFrame],
    variables: list[str],
    survey_years: list[int],
) -> dict[str, dict[Literal["data", "index_variables"], pd.DataFrame | list[str]]]:
    dataset_merging_information = {}
    for module_name, full_data in modules.items():
        idx_vars = [v for v in POTENTIAL_INDEX_VARIABLES if v in full_data.columns]
        mod_vars = [
            v for v in variables if v in full_data.columns and v not in idx_vars
        ]
        # No need to keep module around if we do not have any variables to merge
        if not mod_vars:
            continue
        data = full_data[idx_vars + mod_vars].dropna(axis="index", subset=mod_vars)
        if "survey_year" in idx_vars:
            data = data.query(f"survey_year in {survey_years}")
        dataset_merging_information[module_name] = {
            "data": data,
            "index_variables": idx_vars,
        }
    return _sort_dataset_merging_information(dataset_merging_information)


def _merge_data(
    merging_information: dict[
        str, dict[Literal["data", "index_variables"], pd.DataFrame | list[str]]
    ],
) -> pd.DataFrame:
    out = None
    for i, m in enumerate(merging_information.values()):
        out = m["data"] if i == 0 else out.merge(m["data"], how="outer")
    return out.reset_index(drop=True)
