from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Annotated

import pandas as pd
from pytask import Product, task

from soep_cleaning.config import BLD, SRC


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
    "pbrutto",
    "pequiv",
    "pgen",
    "pkal",
    "pl",
    "ppathl",
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
        raw_data = pd.read_stata(depends_on["dataset"])
        module = SourceFileLoader(
            depends_on["script"].stem,
            str(depends_on["script"]),
        ).load_module()
        cleaned = getattr(module, f"{dataset_name}")(raw_data)
        cleaned.to_pickle(produces)
