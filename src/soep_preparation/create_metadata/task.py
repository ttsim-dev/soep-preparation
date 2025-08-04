"""Tasks to create metadata."""

from typing import Annotated, Any

import pandas as pd
from pytask import task

from soep_preparation.config import DATA_CATALOGS
from soep_preparation.utilities.error_handling import (
    fail_if_input_has_invalid_type,
)


def _create_name_to_data_mapping() -> dict[str, pd.DataFrame]:
    """Mapping of data file and combined variable names to corresponding data."""
    single_data_files = dict(
        DATA_CATALOGS["cleaned_variables"]._entries.items()  # noqa: SLF001
    )
    combined_variables = dict(
        DATA_CATALOGS["combined_variables"]._entries.items()  # noqa: SLF001
    )

    return single_data_files | combined_variables


def _create_name_to_metadata_mapping(
    metadata_names: list[str],
) -> dict[str, pd.DataFrame]:
    """Mapping of names to corresponding metadata."""
    return {name: DATA_CATALOGS["metadata"][name] for name in metadata_names}


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


def _create_variable_to_metadata_name_mapping(data: dict) -> dict[str, str]:
    """Create a mapping of variable names to metadata names.

    Args:
        data: A dictionary containing metadata entries.

    Returns:
        A mapping of variable names to metadata names.
    """
    mapping = {}
    for metadata_name, metadata in data.items():
        variable_names = metadata["variable_dtypes"].keys()
        for variable_name in variable_names:
            mapping[variable_name] = metadata_name
    return mapping


MAP_NAME_TO_DATA = _create_name_to_data_mapping()


for name, data in MAP_NAME_TO_DATA.items():

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


MAP_NAME_TO_METADATA = _create_name_to_metadata_mapping(MAP_NAME_TO_DATA.keys())


def task_create_variable_to_metadata_name_mapping(
    map_name_to_metadata: Annotated[dict[str, pd.DataFrame], MAP_NAME_TO_METADATA],
) -> Annotated[dict[str, str], DATA_CATALOGS["metadata"]["mapping"]]:
    """Create a mapping of variable names to metadata names.

    Args:
        map_name_to_metadata: A dictionary containing single metadata entries.

    Returns:
        A mapping of variable names to data names.

    Raises:
        TypeError: If input data or data name is not of expected type.
    """
    _error_handling_mapping_task(map_name_to_metadata)
    return _create_variable_to_metadata_name_mapping(map_name_to_metadata)


def _error_handling_mapping_task(mapping: Any) -> None:
    fail_if_input_has_invalid_type(input_=mapping, expected_dtypes=["dict"])
    for metadata_name, metadata in mapping.items():
        fail_if_input_has_invalid_type(input_=metadata_name, expected_dtypes=["str"])
        fail_if_input_has_invalid_type(input_=metadata, expected_dtypes=["dict"])
