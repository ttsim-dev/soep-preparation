from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Annotated

import pandas as pd
from pytask import Product, task

from soep_cleaning.config import BLD, SRC


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
            "dataset": SRC.joinpath("data", "V37", f"{dataset}.dta").resolve(),
            "script": SRC.joinpath(
                "initial_preprocessing",
                f"{_dataset_script_name(dataset)}.py",
            ).resolve(),
        },
        "produces": BLD.joinpath(
            "python",
            "data",
            f"{dataset}_long_and_cleaned.pkl",
        ).resolve(),
        "dataset_name": dataset,
    }


for dataset in [
    "biobirth",
    "bioedu",
    "biol",
    "design",
    "hgen",
    "hl",
    "hpathl",
    "hwealth",
    "kidlong",
    # "pbrutto",
    "pequiv",
    # "pgen",
    # "pkal",
    # "pl",
    # "ppathl",
]:

    @task(id=dataset, kwargs=_create_parametrization(dataset))
    def clean_one_dataset(
        depends_on: dict[str, Path],
        produces: Annotated[Path, Product],
        dataset_name: str,
    ) -> None:
        """Cleans a dataset using a specified cleaning script.

        Parameters:
            depends_on (dict[str, Path]): A dictionary containing the paths to the dependencies of the cleaning task.
            produces (Annotated[Path, Product]): The path where the cleaned dataset will be saved.
            dataset_name (str): The name of the dataset to be cleaned.

        Returns:
            None

        """
        _error_handling_task(depends_on, produces, dataset_name)
        raw_data = pd.read_stata(depends_on["dataset"])
        module = SourceFileLoader(
            depends_on["script"].stem,
            str(depends_on["script"]),
        ).load_module()
        cleaned = getattr(module, f"{dataset_name}")(raw_data)
        cleaned.to_pickle(produces)


def _error_handling_task(depends_on, produces, dataset_name):
    _fail_if_missing_dependency(depends_on)
    _fail_if_invalid_dependency(depends_on)
    _fail_if_invalid_input(depends_on, "dict")
    _fail_if_invalid_input(produces, "pathlib.PosixPath")
    _fail_if_invalid_input(dataset_name, "str")
