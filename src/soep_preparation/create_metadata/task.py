"""Tasks to create metadata."""

from typing import Annotated, Any

import pandas as pd
from pytask import task

from soep_preparation.config import DATA_CATALOGS
from soep_preparation.utilities.error_handling import (
    fail_if_input_has_invalid_type,
)


def _get_index_variables(
    dataset: pd.DataFrame,
    potential_index_variables: list[str],
) -> dict:
    return {
        col: dtype_
        for col, dtype_ in dataset.dtypes.items()
        if col in potential_index_variables
    }


def _get_variable_dtypes(
    dataset: pd.DataFrame,
    potential_index_variables: list[str],
) -> dict:
    return {
        col: dtype_.name
        for col, dtype_ in dataset.dtypes.items()
        if col not in potential_index_variables
    }


def _create_metadata_mapping(metadata: dict) -> dict[str, str]:
    """Create a mapping of column names to data file names.

    Args:
        metadata: A dictionary containing metadata entries.

    Returns:
        A mapping of variable names to data file names.
    """
    mapping = {}
    for data_name, data in metadata._entries.items():  # noqa: SLF001
        if (
            data_name not in DATA_CATALOGS["combined_variables"]._entries  # noqa: SLF001
        ) and (
            data_name not in DATA_CATALOGS["cleaned_variables"]._entries  # noqa: SLF001
        ):
            # Skip if data_name is neither among combined variables nor among data files
            continue
        variable_names = data.load()["variable_dtypes"].keys()
        for variable_name in variable_names:
            mapping[variable_name] = data_name
    return mapping


single_data_files = dict(
    DATA_CATALOGS["cleaned_variables"]._entries.items()  # noqa: SLF001
)
combined_variables = dict(
    DATA_CATALOGS["combined_variables"]._entries.items()  # noqa: SLF001
)


for name, data in (single_data_files | combined_variables).items():

    @task(id=name)
    def task_create_metadata(
        data: Annotated[pd.DataFrame, data],
    ) -> Annotated[dict, DATA_CATALOGS["metadata"][name]]:
        """Create metadata for DataFrame.

        Args:
            data: The data to create metadata for.

        Returns:
            Metadata information for DataFrame.

        Raises:
            TypeError: If input data is not of expected type.
        """
        fail_if_input_has_invalid_type(
            input_=data, expected_dtypes=["pandas.core.frame.DataFrame"]
        )
        potential_index_variables = ["p_id", "hh_id", "hh_id_original", "survey_year"]
        index_variables = _get_index_variables(
            dataset=data, potential_index_variables=potential_index_variables
        )
        variable_dtypes = _get_variable_dtypes(
            dataset=data, potential_index_variables=potential_index_variables
        )
        return {
            "index_variables": index_variables,
            "variable_dtypes": variable_dtypes,
        }


@task(after="task_create_metadata")
def task_create_metadata_mapping(
    single_metadata_mapping: Annotated[dict, DATA_CATALOGS["metadata"]],
) -> Annotated[dict[str, dict], DATA_CATALOGS["metadata"]["variable_mapping"]]:
    """Create a mapping of variable names to data file names.

    Args:
        single_metadata_mapping: A dictionary containing single metadata entries.

    Returns:
        A mapping of variable names to data file names.

    Raises:
        TypeError: If input data or data name is not of expected type.
    """
    _error_handling_mapping_task(single_metadata_mapping)
    return _create_metadata_mapping(single_metadata_mapping)


def _error_handling_mapping_task(mapping: Any) -> None:
    fail_if_input_has_invalid_type(
        input_=mapping, expected_dtypes=["_pytask.data_catalog.DataCatalog"]
    )
    for data_name, data in mapping._entries.items():  # noqa: SLF001
        fail_if_input_has_invalid_type(input_=data_name, expected_dtypes=["str"])
        fail_if_input_has_invalid_type(
            input_=data, expected_dtypes=["_pytask.nodes.PickleNode"]
        )
