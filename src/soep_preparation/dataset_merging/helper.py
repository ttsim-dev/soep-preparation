"""Helper functions for merging variables to dataset."""

from difflib import get_close_matches

import pandas as pd
from pytask import PNode, PProvisionalNode

from soep_preparation.config import DATA_CATALOGS, SURVEY_YEARS
from soep_preparation.utilities.error_handling import (
    fail_if_empty,
    fail_if_input_has_invalid_type,
)


def create_dataset(
    variables: list[str],
    min_survey_year: int | None = None,
    max_survey_year: int | None = None,
    survey_years: list[int] | None = None,
    map_variable_to_module: (dict[str, list[str]] | PNode | PProvisionalNode) = {},  # noqa : B006
) -> pd.DataFrame:
    """Create a dataset by merging variables.

    A list of variables and timeframe needs to be specified to create a dataset.
    Variables are results of the pipeline of cleaning and combining variables.

    Args:
        variables: A list of variable names for the merged dataset to contain.
        min_survey_year: Minimum survey year to be included.
        max_survey_year: Maximum survey year to be included.
        survey_years: Survey years to be included in the dataset.
        Either `min_survey_year` and `max_survey_year` or
        `survey_years` must be provided.
        map_variable_to_module: A mapping of variable names to dataset names.
        Defaults to `DATA_CATALOGS["metadata"]["merged"]`.

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
        Either specify `min_survey_year` and `max_survey_year` or `survey_years`.
        To receive data for just one year (e.g. `2025`) either input
        `min_survey_year=2025` and `max_survey_year=2025` or `survey_years=[2025]`.
        Otherwise, `min_survey_year=2024` and `max_survey_year=2025`
        both return a merged dataset with information from the two survey years.
        `map_variable_to_module` is created automatically by the pipeline,
        it can be accessed and provided to the function at
        `DATA_CATALOGS["metadata"]["mapping"]`.
        Specify `merging_behavior` to control the creation of the dataset
        from the different variables.
        The default value is "outer" and sufficient for most cases.

    Examples:
        For an example see `task_example.py`.
    """
    _error_handling(
        map_variable_to_module=map_variable_to_module,
        variables=variables,
        min_survey_year=min_survey_year,
        max_survey_year=max_survey_year,
        survey_years=survey_years,
    )

    survey_years, variables = _fix_user_input(
        survey_years=survey_years,
        min_survey_year=min_survey_year,
        max_survey_year=max_survey_year,
        variables=variables,
    )
    dataset_merging_information = _get_sorted_dataset_merging_information(
        map_variable_to_module,
        variables,
        survey_years,
    )

    return _merge_variables(
        merging_information=dataset_merging_information,
    )


def _error_handling(
    map_variable_to_module: dict[str, list[str]],
    variables: list[str],
    min_survey_year: int | None,
    max_survey_year: int | None,
    survey_years: list[int] | None,
) -> None:
    fail_if_input_has_invalid_type(
        input_=map_variable_to_module,
        expected_dtypes=["dict", "PNode", "PProvisionalNode"],
    )
    fail_if_input_has_invalid_type(input_=variables, expected_dtypes=["list"])
    fail_if_input_has_invalid_type(
        input_=min_survey_year, expected_dtypes=("int", "None")
    )
    fail_if_input_has_invalid_type(
        input_=max_survey_year, expected_dtypes=("int", "None")
    )
    fail_if_input_has_invalid_type(
        input_=survey_years, expected_dtypes=("list", "None")
    )
    fail_if_empty(input_=map_variable_to_module, name="map_variable_to_module")
    fail_if_empty(variables, name="variables")
    _fail_if_missing_survey_year_inputs(survey_years, min_survey_year, max_survey_year)
    if survey_years is not None:
        _fail_if_survey_years_not_valid(
            survey_years=survey_years,
            valid_survey_years=SURVEY_YEARS,
        )
    elif min_survey_year is not None and max_survey_year is not None:
        _fail_if_survey_years_not_valid(
            survey_years=[*range(min_survey_year, max_survey_year + 1)],
            valid_survey_years=SURVEY_YEARS,
        )
        _fail_if_min_larger_max(min_survey_year, max_survey_year)
    _fail_if_invalid_variable(
        variables=variables, map_variable_to_module=map_variable_to_module
    )


def _fail_if_missing_survey_year_inputs(
    survey_years: list[int] | None,
    min_survey_year: int | None,
    max_survey_year: int | None,
) -> None:
    if survey_years is None and (min_survey_year is None or max_survey_year is None):
        msg = """Either survey_years or both min_survey_year and max_survey_year
        need to be provided."""
        raise ValueError(msg)


def _fail_if_invalid_variable(
    variables: list[str],
    map_variable_to_module: dict[str, list[str]],
) -> None:
    for variable in variables:
        if variable not in map_variable_to_module:
            closest_matches = get_close_matches(
                variable,
                map_variable_to_module.keys(),
                n=3,
                cutoff=0.6,
            )
            matches = {
                match: map_variable_to_module[match] for match in closest_matches
            }
            msg = f"""variable {variable} not found in any data file.
            The closest matches with the corresponding data files are:
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


def _fail_if_min_larger_max(min_survey_year: int, max_survey_year: int) -> None:
    if min_survey_year > max_survey_year:
        msg = f"""Expected min survey year to be smaller than max survey year,
        got {(min_survey_year, max_survey_year)} instead."""
        raise ValueError(msg)


def _get_data_file_name_to_variables_mapping(
    map_variable_to_module: dict[str, str],
    variables: list[str],
) -> dict[str, list[str]]:
    data_file_name_to_variables_mapping = {}
    for variable in variables:
        if variable in map_variable_to_module:
            data_file_name = map_variable_to_module[variable]["module"]
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
    min_survey_year: int | None,
    max_survey_year: int | None,
    variables: list[str],
) -> tuple[list[int], list[str]]:
    if (
        survey_years is None
        and min_survey_year is not None
        and max_survey_year is not None
    ):
        survey_years = [
            *range(min_survey_year, max_survey_year + 1),
        ]
    id_variables = ["hh_id", "hh_id_original", "p_id", "survey_year"]
    if any(id_variable in variables for id_variable in id_variables):
        variables = [col for col in variables if col not in id_variables]
    return survey_years, variables


def _get_sorted_dataset_merging_information(
    map_variable_to_module: dict[str, dict],
    variables: list,
    survey_years: list[int],
) -> dict[str, dict]:
    data_mapping = _get_data_file_name_to_variables_mapping(
        map_variable_to_module,
        variables,
    )

    dataset_merging_information = {}
    for data_name, data_variables in data_mapping.items():
        raw_data = (
            DATA_CATALOGS["cleaned_modules"][data_name].load()
            if data_name in DATA_CATALOGS["cleaned_modules"]._entries  # noqa: SLF001
            else DATA_CATALOGS["combined_modules"][data_name].load()
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
