"""Functions to create datasets for pre-processed datasets.

Functions:
- task_manipulate_one_dataset: Calls the dataset specific script.

Usage:
    Import this module and call task_manipulate_one_dataset
    to generate new variables for the relevant datasets.
"""

from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Annotated

import pandas as pd
from pytask import task

from soep_preparation.config import DATA_CATALOGS, SRC, get_dataset_names
from soep_preparation.utilities.error_handling import fail_if_invalid_input

dataset_names = get_dataset_names(SRC / "create_derived_variables")
for name, catalog in DATA_CATALOGS["single_datasets"].items():
    if name not in dataset_names:
        # skipping datasets that do not have a derive variables script
        continue

    @task(id=name)
    def task_create_derived_variables(
        clean_data: Annotated[pd.DataFrame, catalog["cleaned"]],
        script_path: Annotated[
            Path,
            SRC / "create_derived_variables" / f"{name}.py",
        ],
    ) -> Annotated[pd.DataFrame, catalog["derived_variables"]]:
        """Creates derived variables for a dataset using a specified script.

        Parameters:
            clean_data (pd.DataFrame): Cleaned dataset to derive variables for.
            script_path (Path): The path to the script.

        Returns:
            pd.DataFrame: Derived variables to store in the data catalog.
        """
        _error_handling_creation_task(clean_data, script_path)
        module = SourceFileLoader(
            script_path.stem,
            str(script_path),
        ).load_module()
        return module.create_derived_variables(data=clean_data)

    @task(id=name)
    def task_merge_derived_variables(
        clean_data: Annotated[pd.DataFrame, catalog["cleaned"]],
        derived_variables: Annotated[pd.DataFrame, catalog["derived_variables"]],
    ) -> Annotated[pd.DataFrame, catalog["merged"]]:
        """Merge the cleaned and derived variables datasets.

        Args:
            clean_data (pd.DataFrame): The cleaned dataset.
            derived_variables (pd.DataFrame): The derived variables dataset.

        Returns:
            pd.DataFrame: The merged dataset.
        """
        _error_handling_merging_task(clean_data, derived_variables)
        return pd.concat(objs=[clean_data, derived_variables], axis=1)


def _error_handling_creation_task(data, script_path):
    fail_if_invalid_input(data, "pandas.core.frame.DataFrame")
    fail_if_invalid_input(script_path, "pathlib._local.PosixPath")


def _error_handling_merging_task(data, variables):
    fail_if_invalid_input(data, "pandas.core.frame.DataFrame")
    fail_if_invalid_input(variables, "pandas.core.frame.DataFrame")
