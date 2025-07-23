"""Helper functions for merging variables to dataset."""

from difflib import get_close_matches

import pandas as pd

from soep_preparation.config import DATA_CATALOGS, ID_VARIABLES, SURVEY_YEARS
from soep_preparation.utilities.error_handling import (
    fail_if_input_has_invalid_type,
)


def create_dataset(
    variables: list[str],
    min_and_max_survey_years: tuple[int, int] | None = None,
    survey_years: list[int] | None = None,
) -> pd.DataFrame:
    """Create a dataset by merging different specified variables.

    A list of variables and timeframe needs to be specified to create a dataset.
    Variables are results of the pipeline of cleaning and deriving further variables.

    Args:
        variables: A list of variable names for the merged dataset to contain.
        min_and_max_survey_years: Range of survey years.
        survey_years: Survey years to be included in the dataset.
        Either `survey_years` or `min_and_max_survey_years` must be provided.

    Returns:
        The dataset with specified variables and survey years.

    Raises:
        TypeError: If the input types are not as expected.
        ValueError: If variables or variable mapping are empty or
        contain faulty variables.
        Or if survey years out of survey range.
        Or if inadequate merging behavior.

    Notes:
        `variables` needs to be specified and are the variables
        created, renamed, and derived from the raw SOEP data files
        that will be part of the merged dataset.
        Either specify `min_and_max_survey_years` or `survey_years`.
        To receive data for just one year (e.g. `2025`) either input
        `min_and_max_survey_years=(2025,2025)` or `survey_years=[2025]`.
        Otherwise, `min_and_max_survey_years=(2024,2025)`
        and `survey_years=[2024, 2025]`
        both return a merged dataset with information from the two survey years.

    Examples:
        For an example see `task_example.py`.
    """
    variable_mapping = DATA_CATALOGS["metadata"]["variable_mapping"].load()
    _error_handling(
        variables,
        min_and_max_survey_years,
        survey_years,
        variable_mapping,
    )

    survey_years, variables = _fix_user_input(
        survey_years,
        min_and_max_survey_years,
        variables,
    )

    merging_information = _get_sorted_dataset_merging_information(
        variables,
        survey_years,
        variable_mapping,
    )

    return _merge_variables(merging_information)


def _error_handling(
    variables: list[str],
    min_and_max_survey_years: tuple[int, int] | None,
    survey_years: list[int] | None,
    variable_mapping: dict[str, list[str]],
) -> None:
    fail_if_input_has_invalid_type(input_=variables, expected_dtypes=["list"])
    fail_if_input_has_invalid_type(
        input_=min_and_max_survey_years, expected_dtypes=("tuple", "None")
    )
    fail_if_input_has_invalid_type(
        input_=survey_years, expected_dtypes=("list", "None")
    )
    _fail_if_empty(variables)
    if survey_years is not None:
        _fail_if_survey_years_not_valid(
            survey_years=survey_years, valid_survey_years=SURVEY_YEARS
        )
    else:
        _fail_if_survey_years_not_valid(
            survey_years=min_and_max_survey_years, valid_survey_years=SURVEY_YEARS
        )
        _fail_if_min_larger_max(min_and_max_survey_years)
    _fail_if_invalid_variable(variables=variables, variable_mapping=variable_mapping)


def _fail_if_invalid_variable(
    variables: list[str],
    variable_mapping: dict[str, list[str]],
) -> None:
    for variable in variables:
        if (variable not in variable_mapping) and (variable not in ID_VARIABLES):
            closest_matches = get_close_matches(
                variable,
                variable_mapping.keys(),
                n=3,
                cutoff=0.6,
            )
            matches = {match: variable_mapping[match] for match in closest_matches}
            msg = f"""variable {variable} not found in any data file.
            The closest matches with the corresponding data files are:
            {matches}"""
            raise ValueError(msg)


def _fail_if_empty(input_: dict | list) -> None:
    if len(input_) == 0:
        msg = f"Expected {input_} to be non-empty."
        raise ValueError(msg)


def _fail_if_survey_years_not_valid(
    survey_years: list[int] | tuple[int],
    valid_survey_years: list[int],
) -> None:
    if not all(year in valid_survey_years for year in survey_years):
        msg = f"""Expected survey years to be in {valid_survey_years},
        got {survey_years} instead."""
        raise ValueError(msg)


def _fail_if_min_larger_max(min_and_max_survey_years: tuple[int, int]) -> None:
    if min_and_max_survey_years[0] > min_and_max_survey_years[1]:
        msg = f"""Expected min survey year to be smaller than max survey year,
        got {min_and_max_survey_years} instead."""
        raise ValueError(msg)


def _get_data_file_name_to_variables_mapping(
    variable_mapping: dict[str, str],
    variables: list[str],
) -> dict[str, list[str]]:
    data_file_name_to_variables_mapping = {}
    for variable in variables:
        if variable in variable_mapping:
            data_file_name = variable_mapping[variable]
            if data_file_name not in data_file_name_to_variables_mapping:
                data_file_name_to_variables_mapping[data_file_name] = []
            data_file_name_to_variables_mapping[data_file_name].append(variable)
    return data_file_name_to_variables_mapping


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


def _fix_user_input(
    survey_years: list[int] | None,
    min_and_max_survey_years: tuple[int, int] | None,
    variables: list[str],
) -> tuple[list[int], list[str]]:
    if survey_years is None and min_and_max_survey_years is not None:
        survey_years = [
            *range(min_and_max_survey_years[0], min_and_max_survey_years[1] + 1),
        ]
    if any(id_variable in variables for id_variable in ID_VARIABLES):
        variables = [col for col in variables if col not in ID_VARIABLES]
    return survey_years, variables


def _get_sorted_dataset_merging_information(
    variables: list,
    survey_years: list[int],
    variable_mapping: dict[str, dict],
) -> dict[str, dict]:
    data_mapping = _get_data_file_name_to_variables_mapping(
        variable_mapping,
        variables,
    )

    dataset_merging_information = {}
    for data_name, data_variables in data_mapping.items():
        raw_data = (
            DATA_CATALOGS["data_files"][data_name]["cleaned"].load()
            if data_name in DATA_CATALOGS["data_files"]
            else DATA_CATALOGS["combined_variables"][data_name].load()
        )
        index_variables = sorted(
            DATA_CATALOGS["metadata"][data_name].load()["index_variables"].keys(),
        )
        if "survey_year" in raw_data.columns:
            data = raw_data.query(
                f"{min(survey_years)} <= survey_year <= {max(survey_years)}",
            )[index_variables + data_variables]
        else:
            data = raw_data[index_variables + data_variables]
        dataset_merging_information[data_name] = {
            "data": data,
            "index_variables": index_variables,
        }
    return _sort_dataset_merging_information(dataset_merging_information)


def _merge_variables(
    merging_information: dict[str, dict],
) -> pd.DataFrame:
    dataframes = [dataframe["data"] for dataframe in merging_information.values()]
    out = pd.DataFrame()
    for dataframe in dataframes:
        out = dataframe if out.empty else out.merge(dataframe, how="outer")
    return out.reset_index(drop=True)
