"""Task to merge variables from multiple datasets."""

from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Annotated

import pandas as pd
from pytask import PickleNode, task

from soep_preparation.config import DATA_CATALOGS, SRC
from soep_preparation.utilities import get_cleaned_and_potentially_merged_dataset


def _fail_if_invalid_input(input_, expected_dtype: str):
    if expected_dtype not in str(type(input_)):
        msg = f"Expected {input_} to be of type {expected_dtype}, got {type(input_)}"
        raise TypeError(
            msg,
        )


def _fail_if_too_many_or_few_datasets(datasets: dict, expected_entries: int):
    if len(datasets.keys()) != expected_entries:
        msg = f"Expected {expected_entries} datasets, got {len(datasets.keys())}"
        raise ValueError(
            msg,
        )


def _get_datasets_containing_variable(variable_name: str) -> dict[str, PickleNode]:
    # corresponding module for the variable name (e.g. birth_month.py)
    module = SourceFileLoader(
        variable_name,
        str(SRC / "create_merged_variables" / f"{variable_name}.py"),
    ).load_module()
    # arguments to the merge_variable function (datasets required for the variable)
    dataset_names = [
        dataset_name
        for dataset_name in module.merge_variable.__annotations__
        if dataset_name in DATA_CATALOGS["single_datasets"]
    ]
    # return a mapping of the dataset names to the corresponding clean datasets
    return {
        dataset_name: get_cleaned_and_potentially_merged_dataset(
            DATA_CATALOGS["single_datasets"][dataset_name],
        )
        for dataset_name in dataset_names
    }


for variable_name, catalog in DATA_CATALOGS["multiple_datasets"].items():
    datasets = _get_datasets_containing_variable(variable_name)

    @task(id=variable_name)
    def task_merge_variable(
        datasets: Annotated[dict[str, pd.DataFrame], datasets],
        script_path: Annotated[
            Path,
            SRC / "create_merged_variables" / f"{variable_name}.py",
        ],
    ) -> Annotated[
        pd.DataFrame,
        catalog["merged"],
    ]:
        """Merge variables for the meta dataset.

        Args:
            datasets (dict): A mapping of dataset names to DataFrames.
            script_path (Path): Script containing the merge_variable function.

        Returns:
            pd.DataFrame: The merged dataset.
        """
        _error_handling_task(datasets, script_path)
        module = SourceFileLoader(
            script_path.stem,
            str(script_path),
        ).load_module()
        return module.merge_variable(**datasets)


def _error_handling_task(datasets, script_path):
    _fail_if_invalid_input(datasets, "dict")
    _fail_if_too_many_or_few_datasets(datasets, 2)
    _fail_if_invalid_input(script_path, "pathlib.PosixPath")
