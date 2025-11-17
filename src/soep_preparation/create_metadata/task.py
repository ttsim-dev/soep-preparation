"""Tasks to create metadata."""

import shutil
from pathlib import Path
from typing import Annotated, Any

import pandas as pd
import yaml
from pytask import Product, task

from soep_preparation.config import BLD, DATA_CATALOGS, MODULE_STRUCTURE, SRC
from soep_preparation.utilities.error_handling import (
    fail_if_empty,
    fail_if_input_has_invalid_type,
)

POTENTIAL_INDEX_VARIABLES = ["p_id", "hh_id", "hh_id_original", "survey_year"]


def _get_index_variables_metadata(
    module: pd.DataFrame,
) -> dict:
    """Get metadata for index variables in the module.

    Args:
        module: The data containing the index variables.

    Returns:
        Dtype information for each index variable.
    """
    return {
        col: dtype_
        for col, dtype_ in module.dtypes.items()
        if col in POTENTIAL_INDEX_VARIABLES
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
    module: pd.DataFrame,
) -> dict:
    """Get metadata for variables in the module.

    Args:
        module: The data containing the variables.

    Returns:
        Metadata for each variable, including dtype and survey year availability.
    """
    columns = module.columns.tolist()
    survey_year_in_columns = "survey_year" in columns
    variables = [col for col in columns if col not in POTENTIAL_INDEX_VARIABLES]

    metadata = {}
    # for each variable/column in data
    for variable in variables:
        # determine dtype of variable
        variable_dtype = module[variable].dtype
        if variable_dtype.name == "category":
            # categorical dtypes are serialized
            serialized_variable_dtype = _serialize_category_dtype(variable_dtype)
        else:
            serialized_variable_dtype = variable_dtype.name

        # determine survey year availability of variable
        variable_survey_years = None
        if survey_year_in_columns:
            variable_survey_years = sorted(
                set(module[["survey_year", variable]].dropna()["survey_year"])
            )

        metadata[variable] = {
            "dtype": serialized_variable_dtype,
            "survey_years": variable_survey_years,
        }

    return metadata


def _create_variable_to_metadata_mapping(
    map_module_to_metadata: dict,
) -> dict[str, dict]:
    """Create a mapping of variable to metadata.

    Args:
        map_module_to_metadata: Map of module to metadata information.

    Returns:
        A mapping of variable to metadata.
    """
    mapping = {}
    for module_name, metadata in map_module_to_metadata.items():
        for variable_name, variable_metadata in metadata["variable_metadata"].items():
            mapping[variable_name] = {"module": module_name} | variable_metadata
    return mapping


for level, module_names in MODULE_STRUCTURE.items():
    for module_name in module_names:

        @task(id=module_name)
        def task_create_metadata(
            module: Annotated[pd.DataFrame, DATA_CATALOGS[level][module_name]],
        ) -> Annotated[dict, DATA_CATALOGS["metadata"][module_name]]:
            """Create metadata for a single module.

            Args:
                module: The data module to create metadata for.

            Returns:
                Metadata information for index and variables contained in the module.

            Raises:
                TypeError: If input data is not of expected type.
            """
            fail_if_input_has_invalid_type(
                input_=module, expected_dtypes=["pandas.core.frame.DataFrame"]
            )
            index_variables_metadata = _get_index_variables_metadata(module)
            variable_metadata = _get_variable_metadata(module)
            return {
                "index_variables": index_variables_metadata,
                "variable_metadata": variable_metadata,
            }


def task_create_variable_to_metadata_mapping(
    modules_metadata: Annotated[
        dict[str, pd.DataFrame], DATA_CATALOGS["metadata"]._entries
    ],
    path: Annotated[Path, Product] = BLD / "variable_to_metadata_mapping.yaml",
) -> None:
    """Create a mapping of variables to metadata and store as YAML file.

    Args:
        modules_metadata: Map of module to metadata information.
        path: The path to the YAML file to write.

    Raises:
        TypeError: If input data or data name is not of expected type.
    """
    _error_handling_mapping_task(modules_metadata)
    mapping = _create_variable_to_metadata_mapping(modules_metadata)
    with Path.open(path, "w", encoding="utf-8") as file:
        yaml.dump(mapping, file, encoding="utf-8", allow_unicode=True)


def task_copy_yaml_mapping_to_src(
    in_path: Annotated[Path, BLD / "variable_to_metadata_mapping.yaml"],
    out_path: Annotated[Path, Product] = SRC
    / "create_metadata"
    / "variable_to_metadata_mapping.yaml",
) -> None:
    """Copy the YAML mapping file from BLD to SRC."""
    shutil.copy(in_path, out_path)


def _error_handling_mapping_task(modules_metadata: Any) -> None:
    fail_if_input_has_invalid_type(input_=modules_metadata, expected_dtypes=["dict"])
    fail_if_empty(input_=modules_metadata, name="modules_metadata")
    for metadata_name, metadata in modules_metadata.items():
        fail_if_input_has_invalid_type(input_=metadata_name, expected_dtypes=["str"])
        fail_if_input_has_invalid_type(input_=metadata, expected_dtypes=["dict"])
