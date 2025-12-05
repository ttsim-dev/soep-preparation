"""Helper functions for merging variables to dataset."""

from difflib import get_close_matches

import pandas as pd

from soep_preparation.utilities.error_handling import (
    fail_if_empty,
    fail_if_input_has_invalid_type,
)

ID_VARIABLES = ["hh_id", "hh_id_original", "p_id", "survey_year"]


def create_dataset(  # noqa: PLR0913
    modules: dict[str, pd.DataFrame],
    variable_to_metadata: dict[str, dict],
    variables: list[str],
    min_survey_year: int | None = None,
    max_survey_year: int | None = None,
    survey_years: list[int] | None = None,
) -> pd.DataFrame:
    """Create a dataset by merging variables.

    A list of variables and timeframe needs to be specified to create a dataset.
    Variables are results of the pipeline of cleaning and combining variables.

    Args:
        modules: A mapping of cleaned and combined modules.
        variable_to_metadata: A mapping of variable names to dataset names.
        variables: A list of variable names for the merged dataset to contain.
        min_survey_year: Minimum survey year to be included.
        max_survey_year: Maximum survey year to be included.
        survey_years: Survey years to be included in the dataset.
        Either `min_survey_year` and `max_survey_year` or
        `survey_years` must be provided.

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
        `variable_to_metadata` is created automatically by the pipeline,
        it can be accessed and provided to the function at
        `DATA_CATALOGS["metadata"]["mapping"]`.
        Specify `merging_behavior` to control the creation of the dataset
        from the different variables.
        The default value is "outer" and sufficient for most cases.

    Examples:
        For an example see `task_example.py`.
    """
    _error_handling(
        modules=modules,
        variable_to_metadata=variable_to_metadata,
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
        modules=modules,
        variable_to_metadata=variable_to_metadata,
        variables=variables,
        survey_years=survey_years,
    )

    return _merge_variables(
        merging_information=dataset_merging_information,
    )


def _error_handling(  # noqa: PLR0913
    modules: dict[str, pd.DataFrame],
    variable_to_metadata: dict[str, list[str]],
    variables: list[str],
    min_survey_year: int | None,
    max_survey_year: int | None,
    survey_years: list[int] | None,
) -> None:
    fail_if_input_has_invalid_type(
        input_=variable_to_metadata,
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
    fail_if_empty(input_=variable_to_metadata, name="variable_to_metadata")
    fail_if_empty(variables, name="variables")

    # we need at-least one module with the variable `survey_year` to check for
    # valid survey years
    valid_survey_years = modules["pl"]["survey_year"].unique().tolist()
    _fail_if_missing_survey_year_inputs(survey_years, min_survey_year, max_survey_year)
    if survey_years is not None:
        _fail_if_survey_years_not_valid(
            survey_years=survey_years,
            valid_survey_years=valid_survey_years,
        )
    elif min_survey_year is not None and max_survey_year is not None:
        _fail_if_survey_years_not_valid(
            survey_years=[*range(min_survey_year, max_survey_year + 1)],
            valid_survey_years=valid_survey_years,
        )
        _fail_if_min_larger_max(min_survey_year, max_survey_year)
    _fail_if_invalid_variable(
        variables=variables, variable_to_metadata=variable_to_metadata
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
    variable_to_metadata: dict[str, list[str]],
) -> None:
    for variable in variables:
        if variable not in variable_to_metadata:
            closest_matches = get_close_matches(
                variable,
                variable_to_metadata.keys(),
                n=3,
                cutoff=0.6,
            )
            matches = {match: variable_to_metadata[match] for match in closest_matches}
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


def _fix_user_input(
    survey_years: list[int] | None,
    min_survey_year: int | None,
    max_survey_year: int | None,
    variables: list[str],
) -> tuple[list[int], list[str]]:
    # TODO (@hmgaudecker): Are you ok with this non-pure function?  # noqa: TD003
    if (
        survey_years is None
        and min_survey_year is not None
        and max_survey_year is not None
    ):
        survey_years = [
            *range(min_survey_year, max_survey_year + 1),
        ]
    if any(id_variable in variables for id_variable in ID_VARIABLES):
        variables = [col for col in variables if col not in ID_VARIABLES]
    return survey_years, variables


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
        # I'm unhappy with `module_data`.
        # Should we get rid off "module_" prefix in the loop variable names?
        module_data = modules[module_name]
        # instead of using a global constant `ID_VARIABLES` here,
        # I could read the index variables from the metadata of each module
        index_variables = [col for col in module_data.columns if col in ID_VARIABLES]
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


def _merge_variables(
    merging_information: dict[str, dict],
) -> pd.DataFrame:
    dataframes = [dataframe["data"] for dataframe in merging_information.values()]
    out = pd.DataFrame()
    for dataframe in dataframes:
        out = dataframe if out.empty else out.merge(dataframe, how="outer")
    return out.reset_index(drop=True)
