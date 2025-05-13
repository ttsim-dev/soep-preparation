"""Tasks to create metadata for datasets."""

from typing import Annotated

import pandas as pd
from pytask import task

from soep_preparation.config import DATA_CATALOGS
from soep_preparation.utilities import get_cleaned_and_potentially_merged_dataset


def _get_index_columns(
    dataset: pd.DataFrame,
    potential_index_columns: list[str],
) -> dict:
    return {
        col: dtype_
        for col, dtype_ in dataset.dtypes.items()
        if col in potential_index_columns
    }


def _get_column_dtypes(
    dataset: pd.DataFrame,
    potential_index_columns: list[str],
) -> dict:
    # TODO (@hmgaudecker): do we want to just have the "name" of the dtype (e.g. `uint16[pyarrow]`, `category`) or also the dtype itself (only relevant for categorical data which then return the `CategoricalDtype`)
    return {
        col: dtype_.name
        for col, dtype_ in dataset.dtypes.items()
        if col not in potential_index_columns
    }


def _columns_to_dataset_mapping(datasets: dict) -> dict[str, str]:
    """Map column names to dataset names for given datasets.

    Args:
        datasets (dict): Mapping dataset names to their metadata.

    Returns:
        dict: Mapping column names to list of dataset names contained in.
    """
    dataset_mapping = {}
    for dataset_name, dataset_info in datasets.items():
        columns = dataset_info["metadata"].load()["column_dtypes"].keys()
        for column in columns:
            dataset_mapping[column] = dataset_name
    return dataset_mapping


for dataset_kind in ["single_datasets", "multiple_datasets"]:
    for name, catalog in DATA_CATALOGS[dataset_kind].items():
        if dataset_kind == "single_datasets":
            dataset = get_cleaned_and_potentially_merged_dataset(catalog)
            after = f"task_clean_one_dataset[{name}]"

        else:
            dataset = catalog["merged"]
            after = f"task_merge_variable[{name}]"

        @task(id=name, after=after)
        def task_create_single_metadata(
            dataset: Annotated[pd.DataFrame, dataset],
        ) -> Annotated[dict, DATA_CATALOGS[dataset_kind][name]["metadata"]]:
            """Create metadata for a single dataset.

            Args:
                dataset (pd.DataFrame): The dataset for which to create metadata.

            Returns:
                dict: Metadata information for single dataset.
            """
            potential_index_columns = ["p_id", "hh_id", "hh_id_orig", "survey_year"]
            index_columns = _get_index_columns(dataset, potential_index_columns)
            column_dtypes = _get_column_dtypes(dataset, potential_index_columns)
            return {
                "index_columns": index_columns,
                "column_dtypes": column_dtypes,
            }


@task(after="task_create_single_metadata")
def task_create_column_to_dataset_mapping(
    single_datasets: Annotated[dict, DATA_CATALOGS["single_datasets"]],
    multiple_datasets: Annotated[dict, DATA_CATALOGS["multiple_datasets"]],
) -> Annotated[dict[str, dict], DATA_CATALOGS["merged"]["metadata"]]:
    """Create mapping of column name to dataset names.

    Args:
        single_datasets (dict): Mapping of single
        dataset names to respective DataCatalog.
        multiple_datasets (dict): Mapping of multiple
        dataset names to respective DataCatalog.

    Returns:
        dict: Mapping column names to list of dataset names that contain those columns.
    """
    return {
        "single_datasets": _columns_to_dataset_mapping(single_datasets),
        "multiple_datasets": _columns_to_dataset_mapping(multiple_datasets),
    }
