from pathlib import Path
from typing import Annotated

from pytask import task
from soep_cleaning.config import DATASETS, SOEP_VERSION, SRC, data_catalog, pd


def _iteratively_read_one_dataset(
    itr: pd.io.stata.StataReader,
    list_of_columns: list,
) -> pd.DataFrame:
    """Read a dataset in chunks."""
    value_labels_dict = {col: {} for col in list_of_columns}
    out = pd.DataFrame()
    for chunk in itr:
        for col in list_of_columns:
            value_labels = {}
            if col in itr.value_labels():
                value_labels = itr.value_labels()[col]
            unique_values = {
                value: value
                for value in chunk[col].unique()
                if value not in value_labels.values()
            }
            value_labels_dict[col] = (
                value_labels_dict[col] | value_labels | unique_values
            )

        out = pd.concat([out, chunk], ignore_index=True)
    cat_dtypes = {
        col: pd.CategoricalDtype(
            categories=list(dict(sorted(value_dict.items())).values()),
            ordered=True,
        )
        for col, value_dict in value_labels_dict.items()
    }
    return out.astype(cat_dtypes)


for dataset in DATASETS:

    @task(id=dataset)
    def task_pickle_one_dataset(
        orig_data: Annotated[Path, SRC.joinpath(f"data/{SOEP_VERSION}/{dataset}.dta")],
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
            convert_categoricals=False,
        ) as itr:
            return _iteratively_read_one_dataset(itr, list_of_columns)
