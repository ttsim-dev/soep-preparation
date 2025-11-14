"""Functions to create combined variables."""

from typing import Annotated, Any

import pandas as pd
from pytask import PickleNode, task

from soep_preparation.config import DATA_CATALOGS, SRC
from soep_preparation.utilities.error_handling import fail_if_input_has_invalid_type
from soep_preparation.utilities.general import (
    get_script_names,
    get_variable_names_in_script,
    load_script,
)


def _fail_if_too_many_or_too_few_dataframes(
    dataframes: dict, expected_entries: int
) -> None:
    if len(dataframes.keys()) != expected_entries:
        msg = f"Expected {expected_entries} dataframes, got {len(dataframes.keys())}"
        raise ValueError(
            msg,
        )


def _get_relevant_data_files_mapping(
    function_: Any,
) -> dict[str, PickleNode]:
    # get the relevant data file names from the function annotations
    relevant_data_file_names = [
        data_file_name
        for data_file_name in function_.__annotations__
        if data_file_name in DATA_CATALOGS["cleaned_modules"]._entries  # noqa: SLF001
    ]
    # create a mapping of data file names to DataFrames
    # using the data catalog
    return {
        data_name: DATA_CATALOGS["cleaned_modules"][data_name]
        for data_name in relevant_data_file_names
    }


script_names = get_script_names(SRC / "combine_modules")
for script_name in script_names:
    script = load_script(SRC / "combine_modules" / f"{script_name}.py")
    variable_names = get_variable_names_in_script(script)
    for variable_name in variable_names:
        function_ = getattr(script, f"derive_{variable_name}")
        map_data_file_name_to_data = _get_relevant_data_files_mapping(
            function_=function_
        )

        @task(id=variable_name)
        def task_create_combined_modules(
            map_data_file_name_to_data: Annotated[
                dict[str, pd.DataFrame], map_data_file_name_to_data
            ],
            function_: Annotated[Any, function_],
        ) -> Annotated[pd.DataFrame, DATA_CATALOGS["combined_modules"][variable_name]]:
            """Combine variables from multiple modules.

            Args:
                map_data_file_name_to_data: A mapping of data file names to DataFrames.
                function_: Function to create combined variables.

            Returns:
                Combined variables.

            Raises:
                TypeError: If input data files or function is not of expected type.
                ValueError: If number of dataframes is not as expected.
            """
            _error_handling_derived_variables(
                mapping=map_data_file_name_to_data, function_=function_
            )
            return function_(**map_data_file_name_to_data)


def _error_handling_derived_variables(mapping: Any, function_: Any) -> None:
    fail_if_input_has_invalid_type(input_=mapping, expected_dtypes=["dict"])
    _fail_if_too_many_or_too_few_dataframes(dataframes=mapping, expected_entries=2)
    fail_if_input_has_invalid_type(input_=function_, expected_dtypes=["function"])
