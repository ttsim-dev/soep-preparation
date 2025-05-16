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


def _fail_if_invalid_input(input_, expected_dtype: str):
    if expected_dtype not in str(type(input_)):
        msg = f"Expected {input_} to be of type {expected_dtype}, got {type(input_)}"
        raise TypeError(
            msg,
        )


for name in get_dataset_names(SRC / "create_derived_variables"):
    assert name in DATA_CATALOGS["single_datasets"], (
        f"There is no data catalog entry corresponding to\n"
        f"{SRC / 'create_derived_variables' / name}"
    )

    catalog = DATA_CATALOGS["single_datasets"][name]

    @task(id=name)
    def task_create_derived_variables(
        clean_data: Annotated[Path, catalog["cleaned"]],
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

        Raises:
            FileNotFoundError: If dataset or cleaning script file do not exist.
            ImportError: If there is an error loading the manipulation script module.
            AttributeError: If expected function not in manipulation script.

        """
        _error_handling_task(clean_data, script_path)
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
        return pd.concat(objs=[clean_data, derived_variables], axis=1)


def _error_handling_task(data, script_path):
    _fail_if_invalid_input(data, "pandas.core.frame.DataFrame'")
    _fail_if_invalid_input(script_path, "pathlib._local.PosixPath")
