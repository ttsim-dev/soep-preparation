from typing import Annotated

import pandas as pd
from click import Path
from pytask import task

from soep_preparation.config import DATA_CATALOGS, ROOT


@task(after="task_merge_variables")
def task_copy_dataset_to_root(
    dataset: Annotated[pd.DataFrame, DATA_CATALOGS["merged"]["example_merged_dataset"]],
    dataset_copy_path: Annotated[Path, ROOT / "example_merged_dataset.pickle"],
):
    """Copy the dataset to the root directory.

    Args:
        dataset: The dataset to be copied.
        dataset_copy_path: The path to copy the dataset to.

    Raises:
    """
    dataset.to_pickle(dataset_copy_path)
