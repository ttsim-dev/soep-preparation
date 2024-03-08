import pandas as pd

from pytask import task

from soep_cleaning.config import SRC, BLD
import soep_cleaning.initial_preprocessing.data_specific_cleaner as data_specific_cleaner

def create_parametrization(dataset: str) -> dict:
    """Create the parametrization for the task clean dataset."""
    return {
            "depends_on": SRC.joinpath("data", "V37", f"{dataset}.dta").resolve(),
            "produces": BLD.joinpath("python", "data", f"{dataset}_long_and_cleaned.pkl").resolve(),
            "data_set_name": dataset,
        }

for dataset in ["biobirth", "bioedu", "biol", "design", "hgen", "hl", "hpathl", "hwealth", "kidlong", "pbrutto", "pequiv", "pgen", "pkal", "pl", "ppathl"]:

    @task(id=dataset, kwargs=create_parametrization(dataset))
    def task_clean_one_dataset(depends_on, produces, data_set_name):
        """
        Clean one dataset.
        
        Args:
            depends_on (str): Path to the raw data file.
            produces (str): Path to save the cleaned data file.
            data_set_name (str): Name of the dataset.
        """
        breakpoint()
        raw_data = pd.read_stata(depends_on)
        cleaned = getattr(data_specific_cleaner, f"{data_set_name}")(raw_data)
        cleaned.to_pickle(produces)
