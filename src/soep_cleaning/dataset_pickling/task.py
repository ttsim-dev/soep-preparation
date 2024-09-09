from pathlib import Path
from typing import Annotated

from pytask import task
from soep_cleaning.config import DATASETS, SRC, data_catalog, pd

for dataset in DATASETS:

    @task(id=dataset)
    def task_pickle_one_dataset(
        orig_data: Annotated[Path, SRC.joinpath(f"data/V38/{dataset}.dta")],
        relevant_soep_columns: Annotated[
            Path,
            data_catalog["infos"]["relevant_soep_columns"],
        ],
        dataset: str = dataset,
    ) -> Annotated[pd.DataFrame, data_catalog["raw"][dataset]]:
        """Saves the raw dataset to the data catalog in a more efficient procedure.

        Parameters:
            orig_data (Path): The path to the original dataset.
            dataset (str): The name of the dataset.

        Returns:
            pd.DataFrame: A pandas DataFrame to be saved to the data catalog.

        """
        columns = pd.read_csv(relevant_soep_columns)[dataset]
        list_of_columns = columns[columns.notna()].to_list()

        with pd.read_stata(
            orig_data,
            chunksize=100_000,
            columns=list_of_columns,
        ) as itr:
            out = pd.DataFrame()

            for chunk in itr:
                out = pd.concat([out, chunk], ignore_index=True)

        return out
