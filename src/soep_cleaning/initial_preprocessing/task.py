import pandas as pd
from pytask import task

from soep_cleaning.config import BLD, SRC
from soep_cleaning.initial_preprocessing import data_specific_cleaner


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
    def clean_one_dataset(depends_on, produces, data_set_name=dataset):
        """Clean one dataset.

        Args:
            depends_on (str): Path to the raw data file.
            produces (str): Path to save the cleaned data file.
            data_set_name (str): Name of the dataset.

        """
        raw_data = pd.read_stata(depends_on["dataset"])
        cleaned = getattr(data_specific_cleaner, f"{data_set_name}")(raw_data)
        cleaned.to_pickle(produces)
