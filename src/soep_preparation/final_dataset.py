"""Helper function to create final dataset."""

from difflib import get_close_matches

import pandas as pd

from soep_preparation.config import METADATA
from soep_preparation.utilities.error_handling import (
    fail_if_empty,
    fail_if_input_has_invalid_type,
)

ID_VARIABLES = ["hh_id", "hh_id_original", "p_id", "survey_year"]


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
        variable_to_metadata: A mapping of variable names to dataset names.
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
        `variable_to_metadata` is created automatically by the pipeline,
        it can be accessed and provided to the function at
        `src/soep_preparation/create_metadata/variable_to_metadata_mapping.yaml`.
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
        variable_to_metadata=METADATA,
        variables=variables,
        survey_years=survey_years,
    )
    harmonized_variables = _harmonize_variables(variables)

    dataset_merging_information = _get_sorted_dataset_merging_information(
        modules=modules,
        variable_to_metadata=METADATA,
        variables=harmonized_variables,
        survey_years=survey_years,
    )

    return _merge_data(
        merging_information=dataset_merging_information,
    )


def _error_handling(
    modules: dict[str, pd.DataFrame],
    variable_to_metadata: dict[str, dict],
    variables: list[str],
    survey_years: list[int],
) -> None:
    fail_if_input_has_invalid_type(
        input_=METADATA,
        expected_dtypes=["dict", "PNode", "PProvisionalNode"],
    )
    fail_if_input_has_invalid_type(input_=variables, expected_dtypes=["list"])
    fail_if_input_has_invalid_type(
        input_=survey_years, expected_dtypes=("list", "None")
    )
    fail_if_empty(input_=variable_to_metadata, name="variable_to_metadata")
    fail_if_empty(variables, name="variables")

    # TODO (@hmgaudecker): we need at-least one module with the variable  # noqa: TD003
    # `survey_year` to check for valid survey years
    valid_survey_years = modules["pl"]["survey_year"].unique().tolist()
    _fail_if_survey_years_not_valid(
        survey_years=survey_years,
        valid_survey_years=valid_survey_years,
    )
    _fail_if_invalid_variable(
        variables=variables, variable_to_metadata=variable_to_metadata
    )


def _fail_if_invalid_variable(
    variables: list[str],
    variable_to_metadata: dict[str, dict],
) -> None:
    for variable in variables:
        if variable not in variable_to_metadata and variable not in ID_VARIABLES:
            closest_matches = get_close_matches(
                variable,
                variable_to_metadata.keys(),
                n=3,
                cutoff=0.6,
            )
            matches = {match: variable_to_metadata[match] for match in closest_matches}
            msg = f"""variable {variable} not found in any module.
            The closest matches with the corresponding modules are:
            {matches}"""
            raise ValueError(msg)


def _fail_if_survey_years_not_valid(
    survey_years: list[int],
    valid_survey_years: list[int],
) -> None:
    if not all(year in valid_survey_years for year in survey_years):
        msg = f"""Expected survey years to be in {valid_survey_years},
        got {survey_years} instead."""
        raise ValueError(msg)


def _get_module_to_variable(
    variable_to_metadata: dict[str, dict],
    variables: list[str],
) -> dict[str, list[str]]:
    module_to_variable = {}
    for variable in variables:
        if variable in variable_to_metadata:
            module_name = variable_to_metadata[variable]["module"]
            if module_name not in module_to_variable:
                module_to_variable[module_name] = []
            module_to_variable[module_name].append(variable)
    return module_to_variable


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
    return [v for v in variables if v not in ID_VARIABLES]


def _get_sorted_dataset_merging_information(
    modules: dict[str, pd.DataFrame],
    variable_to_metadata: dict[str, dict],
    variables: list,
    survey_years: list[int],
) -> dict[str, dict]:
    module_to_variable = _get_module_to_variable(
        variable_to_metadata,
        variables,
    )

    dataset_merging_information = {}
    for module_name, module_variables in module_to_variable.items():
        # TODO (@hmgaudecker): I'm unhappy with `module_data`.  # noqa: TD003
        # Should we get rid off "module_" prefix in the loop variable names?
        module_data = modules[module_name]
        index_variables = [v for v in module_data.columns if v in ID_VARIABLES]
        if "survey_year" in index_variables:
            data = module_data.query(
                f"{min(survey_years)} <= survey_year <= {max(survey_years)}",
            )[index_variables + module_variables]
        else:
            data = module_data[index_variables + module_variables]
        dataset_merging_information[module_name] = {
            "data": data,
            "index_variables": index_variables,
        }
    return _sort_dataset_merging_information(dataset_merging_information)


def _merge_data(
    merging_information: dict[str, dict],
) -> pd.DataFrame:
    modules = [module["data"] for module in merging_information.values()]
    out = pd.DataFrame()
    for module in modules:
        out = module if out.empty else out.merge(module, how="outer")
    return out.reset_index(drop=True)
