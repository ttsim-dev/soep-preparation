"""Tasks to create metadata."""

from pathlib import Path
from typing import Annotated, Any

import pandas as pd
import yaml
from pytask import PNode, PProvisionalNode, Product, task

from soep_preparation.config import BLD, DATA_CATALOGS, DATA_ROOT, SOEP_VERSION, SRC
from soep_preparation.utilities.error_handling import (
    fail_if_empty,
    fail_if_input_has_invalid_type,
)
from soep_preparation.utilities.general import (
    get_data_file_names,
    get_script_names,
    get_variable_names_in_module,
    load_module,
)


def _create_name_to_data_mapping() -> dict[str, PNode | PProvisionalNode]:
    """Mapping of data file and combined variable names to corresponding data."""
    single_data_file_names = get_data_file_names(
        SRC / "clean_variables", data_root=DATA_ROOT, soep_version=SOEP_VERSION
    )
    map_single_data_files = {
        name: DATA_CATALOGS["cleaned_variables"][name]
        for name in single_data_file_names
    }

    script_names = get_script_names(SRC / "combine_variables")
    modules = [
        load_module(SRC / "combine_variables" / f"{script_name}.py")
        for script_name in script_names
    ]
    combined_variable_names = [
        combined_variable_name
        for module in modules
        for combined_variable_name in get_variable_names_in_module(module)
    ]
    map_combined_variables = {
        name: DATA_CATALOGS["combined_variables"][name]
        for name in combined_variable_names
    }

    return map_single_data_files | map_combined_variables


def _create_name_to_metadata_mapping(
    metadata_names: list[str],
) -> dict[str, PNode | PProvisionalNode]:
    """Mapping of metadata names to corresponding metadata.

    Args:
        metadata_names: Names of previously created metadata information.

    Returns:
        A mapping of names to metadata information.
    """
    return {name: DATA_CATALOGS["metadata"][name] for name in metadata_names}


def _get_index_variables_metadata(
    data: pd.DataFrame,
    potential_index_variables: list[str],
) -> dict:
    return {
        col: dtype_
        for col, dtype_ in data.dtypes.items()
        if col in potential_index_variables
    }


def _serialize_category_dtype(variable_dtype: pd.CategoricalDtype) -> dict:
    """Serialize a pandas CategoricalDtype to a dictionary.

    Args:
        variable_dtype: The CategoricalDtype to serialize.

    Returns:
        A dictionary representation of the CategoricalDtype.
    """
    return {
        "categories": variable_dtype.categories.tolist(),
        "categories_dtype": variable_dtype.categories.dtype.name,
        "ordered": variable_dtype.ordered,
    }


def _get_variable_metadata(
    data: pd.DataFrame,
    potential_index_variables: list[str],
) -> dict:
    """Get metadata for variables in the DataFrame.

    Args:
        data: The DataFrame containing the data.
        potential_index_variables: A list of potential index variable names.

    Returns:
        Metadata for each variable, including dtype and survey year availability.
    """
    columns = data.columns.tolist()
    survey_year_in_columns = "survey_year" in columns
    variables = [col for col in columns if col not in potential_index_variables]

    metadata = {}
    # for each variable/column in data
    for variable in variables:
        # determine dtype of variable
        variable_dtype = data[variable].dtype
        if variable_dtype.name == "category":
            # categorical dtypes are serialized
            serialized_variable_dtype = _serialize_category_dtype(variable_dtype)
        else:
            serialized_variable_dtype = variable_dtype.name

        # determine survey year availability of variable
        variable_survey_years = None
        if survey_year_in_columns:
            variable_survey_years = sorted(
                set(data[["survey_year", variable]].dropna()["survey_year"])
            )

        metadata[variable] = {
            "dtype": serialized_variable_dtype,
            "survey_years": variable_survey_years,
        }

    return metadata


def _create_variable_to_metadata_name_mapping(data: dict) -> dict[str, dict]:
    """Create a mapping of variable names to metadata names.

    Args:
        data: A dictionary containing metadata entries.

    Returns:
        A mapping of variable names to metadata names.
    """
    mapping = {}
    for data_file, metadata in data.items():
        for variable_name, variable_metadata in metadata["variable_metadata"].items():
            mapping[variable_name] = {"data_file": data_file} | variable_metadata
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
            Metadata information for index and variable data.

        Raises:
            TypeError: If input data is not of expected type.
        """
        fail_if_input_has_invalid_type(
            input_=data, expected_dtypes=["pandas.core.frame.DataFrame"]
        )
        potential_index_variables = ["p_id", "hh_id", "hh_id_original", "survey_year"]
        index_variables_metadata = _get_index_variables_metadata(
            data=data, potential_index_variables=potential_index_variables
        )
        variable_metadata = _get_variable_metadata(
            data=data, potential_index_variables=potential_index_variables
        )
        return {
            "index_variables": index_variables_metadata,
            "variable_metadata": variable_metadata,
        }


MAP_NAME_TO_METADATA = _create_name_to_metadata_mapping(MAP_NAME_TO_DATA.keys())


def task_create_variable_to_metadata_name_mapping(
    map_name_to_metadata: Annotated[dict[str, pd.DataFrame], MAP_NAME_TO_METADATA],
) -> Annotated[dict[str, dict], DATA_CATALOGS["metadata"]["mapping"]]:
    """Create a mapping of variable names to metadata names.

    Args:
        map_name_to_metadata: Map of metadata names to metadata information.

    Returns:
        A mapping of variable names to metadata names.

    Raises:
        TypeError: If input data or data name is not of expected type.
    """
    _error_handling_mapping_task(map_name_to_metadata)
    return _create_variable_to_metadata_name_mapping(map_name_to_metadata)


def task_yaml_dump_mapping_bld(
    mapping: Annotated[dict[str, dict], DATA_CATALOGS["metadata"]["mapping"]],
    path: Annotated[Path, Product] = BLD / "variable_to_metadata_mapping.yaml",
) -> None:
    """Dump the variable to metadata mapping to a YAML file and store in BLD.

    Args:
        mapping: The mapping of variable names to metadata names.
        path: The path to the YAML file to write.
    """
    with Path.open(path, "w", encoding="utf-8") as file:
        yaml.dump(mapping, file, encoding="utf-8", allow_unicode=True)


def task_yaml_dump_mapping_src(
    mapping: Annotated[dict[str, dict], DATA_CATALOGS["metadata"]["mapping"]],
    path: Annotated[Path, Product] = SRC
    / "dataset_merging"
    / "variable_to_metadata_mapping.yaml",
) -> None:
    """Dump the variable to metadata mapping to a YAML file and store in SRC.

    Args:
        mapping: The mapping of variable names to metadata names.
        path: The path to the YAML file to write.
    """
    with Path.open(path, "w", encoding="utf-8") as file:
        yaml.dump(
            mapping,
            file,
            encoding="utf-8",
            allow_unicode=True,
        )


def _error_handling_mapping_task(mapping: Any) -> None:
    fail_if_input_has_invalid_type(input_=mapping, expected_dtypes=["dict"])
    fail_if_empty(input_=mapping, name="mapping")
    for metadata_name, metadata in mapping.items():
        fail_if_input_has_invalid_type(input_=metadata_name, expected_dtypes=["str"])
        fail_if_input_has_invalid_type(input_=metadata, expected_dtypes=["dict"])
