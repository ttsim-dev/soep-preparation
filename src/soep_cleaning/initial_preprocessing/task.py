from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Annotated

import pandas as pd
from pytask import task

from soep_cleaning.config import SRC, data_catalog


def _fail_if_missing_dependency(depends_on: dict[str, Path]):
    for key, value in depends_on.items():
        if not value.exists():
            msg = f"The dependency {key} does not exist at {value}."
            raise FileNotFoundError(
                msg,
            )


def _fail_if_invalid_dependency(depends_on: dict[str, Path]):
    for key, value in depends_on.items():
        if not isinstance(value, Path):
            msg = f"The dependency {key} is not a Path object, got {type(value)}."
            raise TypeError(
                msg,
            )


def _fail_if_invalid_input(inputt, expected_dtype: str):
    if expected_dtype not in str(type(inputt)):
        msg = f"Expected {inputt} to be of type {expected_dtype}, got {type(inputt)}"
        raise TypeError(
            msg,
        )


def _dataset_script_name(dataset_name: str) -> str:
    """Map the dataset name to the name of the script that cleans the dataset."""
    if dataset_name.startswith("bio"):
        return "bio_specific_cleaner"
    elif dataset_name.startswith("h"):
        return "h_specific_cleaner"
    elif dataset_name.startswith("p"):
        return "p_specific_cleaner"
    else:
        return "other_specific_cleaner"


def _create_parametrization(dataset: str) -> dict:
    """Create the parametrization for the task clean dataset."""
    _fail_if_invalid_input(dataset, "str")
    return {
        "depends_on": {
            "dataset": data_catalog[dataset],
            "script": SRC.joinpath(
                "initial_preprocessing",
                f"{_dataset_script_name(dataset)}.py",
            ).resolve(),
        },
        "dataset_name": dataset,
    }


for dataset in data_catalog["orig"].entries:

    @task(id=dataset)
    def task_clean_one_dataset(
        orig_data: Annotated[Path, data_catalog["orig"][dataset]],
        script_path: Annotated[
            Path,
            Path(
                SRC.joinpath(
                    "initial_preprocessing",
                    f"{_dataset_script_name(dataset)}.py",
                ).resolve(),
            ),
        ],
        dataset: str = dataset,
    ) -> Annotated[pd.DataFrame, data_catalog["cleaned"][dataset]]:
        """Cleans a dataset using a specified cleaning script.

        Parameters:
            orig_data (Path): The path to the dataset to be cleaned.
            script_path (Path): The path to the cleaning script.
            dataset (str): The name of the dataset.

        Returns:
            pd.DataFrame: A cleaned pandas DataFrame to be saved to the data catalog.

        Raises:
            FileNotFoundError: If the dataset file or cleaning script file does not exist.
            ImportError: If there is an error loading the cleaning script module.
            AttributeError: If the cleaning script module does not contain the expected function.

        """
        _error_handling_task(orig_data, script_path)
        module = SourceFileLoader(
            script_path.stem,
            str(script_path),
        ).load_module()
        """With pd.read_stata(orig_data, chunksize=1_000) as itr:

        for chunk in itr:
            getattr(module, f"{dataset}")(chunk)

        """
        return getattr(module, f"{dataset}")(pd.read_stata(orig_data))


def _error_handling_task(orig_data, script_path):
    _fail_if_invalid_input(orig_data, "pathlib.PosixPath")
    _fail_if_invalid_input(script_path, "pathlib.PosixPath")
