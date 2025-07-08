"""Functions to create derived variables."""

from pathlib import Path
from typing import Annotated, Any

import pandas as pd
from pytask import PickleNode, task

from soep_preparation.config import DATA_CATALOGS, SRC
from soep_preparation.utilities.error_handling import fail_if_input_has_invalid_type
from soep_preparation.utilities.general import (
    get_script_names,
    get_stems_if_corresponding_raw_data_file_exists,
    load_module,
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
    # arguments to the function (data files required for the variable)
    data_names = [
        data_name
        for data_name in function_.__annotations__
        if data_name in DATA_CATALOGS["data_files"]
    ]
    # return a mapping of the data file names to the corresponding dataframes
    return {
        data_name: DATA_CATALOGS["derived_variables"][data_name]
        for data_name in data_names
    }


def _get_variable_names_in_module(module: Any) -> list[str]:
    """Get the variable names in the module.

    Args:
        module: The module to get the variable names from.

    Returns:
        The variable names in the module.
    """
    return [
        variable_name.split("derive_")[-1]
        for variable_name in module.__dict__
        if variable_name.startswith("derive_")
    ]


data_file_names = get_stems_if_corresponding_raw_data_file_exists(
    directory=SRC / "combine_variables"
)

for data_file_name, data_file_catalog in DATA_CATALOGS["data_files"].items():
    if data_file_name in data_file_names:
        # data files that have a derive variables script get processed
        @task(id=data_file_name)
        def task_combine_variables(
            clean_data: Annotated[pd.DataFrame, data_file_catalog["cleaned"]],
            script_path: Annotated[
                Path,
                SRC / "combine_variables" / f"{data_file_name}.py",
            ],
        ) -> Annotated[pd.DataFrame, data_file_catalog["derived_variables"]]:
            """Creates derived variables using a specified script.

            Parameters:
                clean_data: Cleaned existing variables to derive variables for.
                script_path: The path to the script.

            Returns:
                Derived variables to store in the data data_file_catalog.

            Raises:
                TypeError: If input data or script path is not of expected type.
            """
            _error_handling_creation_task(data=clean_data, script_path=script_path)
            module = load_module(script_path)
            return module.combine_variables(data=clean_data)

        @task(id=data_file_name)
        def task_merge_derived_variables(
            clean_data: Annotated[pd.DataFrame, data_file_catalog["cleaned"]],
            derived_variables: Annotated[
                pd.DataFrame, data_file_catalog["derived_variables"]
            ],
        ) -> Annotated[
            pd.DataFrame, DATA_CATALOGS["derived_variables"][data_file_name]
        ]:
            """Merge the cleaned and derived variables data.

            Args:
                clean_data: The cleaned existing variables.
                derived_variables: The derived variables.

            Returns:
                The merged data of cleaned existing and derived variables.

            Raises:
                TypeError: If input data or derived variables is not of expected type.
            """
            _error_handling_merging_task(data=clean_data, variables=derived_variables)
            return pd.concat(objs=[clean_data, derived_variables], axis=1)

    else:
        # data files that do not have a derive variables script get copied to
        # the derived variables data_file_catalog
        @task(id=data_file_name)
        def task_copy_cleaned_dtaa(
            clean_data: Annotated[pd.DataFrame, data_file_catalog["cleaned"]],
        ) -> Annotated[
            pd.DataFrame, DATA_CATALOGS["derived_variables"][data_file_name]
        ]:
            """Copy the cleaned data to the derived variables catalog.

            Args:
                clean_data: The cleaned data.

            Returns:
                The copied data.

            Raises:
                TypeError: If input data is not of expected type.
            """
            fail_if_input_has_invalid_type(
                input_=clean_data, expected_dtypes=["pandas.core.frame.DataFrame"]
            )
            return clean_data


script_names = get_script_names(SRC / "combine_variables")
for script_name in script_names:
    if script_name in data_file_names:
        # skipping scripts that have been processed above
        continue
    module = load_module(SRC / "combine_variables" / f"{script_name}.py")
    variable_names = _get_variable_names_in_module(module)
    for variable_name in variable_names:
        function_ = getattr(module, f"derive_{variable_name}")
        data_files = _get_relevant_data_files_mapping(function_=function_)

        @task(id=variable_name)
        def task_create_merged_variables(
            data_files: Annotated[dict[str, pd.DataFrame], data_files],
            function_: Annotated[Any, function_],
        ) -> Annotated[pd.DataFrame, DATA_CATALOGS["derived_variables"][variable_name]]:
            """Merge variables for the meta dataset.

            Args:
                data_files: A mapping of data file names to DataFrames.
                function_: Function to create derived variables.

            Returns:
                Derived variables.

            Raises:
                TypeError: If input data files or function is not of expected type.
                ValueError: If number of dataframes is not as expected.
            """
            _error_handling_derived_variables(data=data_files, function_=function_)
            return function_(**data_files)


def _error_handling_creation_task(data: Any, script_path: Any) -> None:
    fail_if_input_has_invalid_type(
        input_=data, expected_dtypes=["pandas.core.frame.DataFrame"]
    )
    fail_if_input_has_invalid_type(
        input_=script_path, expected_dtypes=["pathlib._local.PosixPath"]
    )


def _error_handling_merging_task(data: Any, variables: Any) -> None:
    fail_if_input_has_invalid_type(
        input_=data, expected_dtypes=["pandas.core.frame.DataFrame"]
    )
    fail_if_input_has_invalid_type(
        input_=variables, expected_dtypes=["pandas.core.frame.DataFrame"]
    )


def _error_handling_derived_variables(data: Any, function_: Any) -> None:
    fail_if_input_has_invalid_type(input_=data, expected_dtypes=["dict"])
    _fail_if_too_many_or_too_few_dataframes(dataframes=data, expected_entries=2)
    fail_if_input_has_invalid_type(input_=function_, expected_dtypes=["function"])
