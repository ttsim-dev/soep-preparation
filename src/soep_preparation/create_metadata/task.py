"""Tasks to create metadata."""

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


def _fail_if_variable_in_multiple_modules(
    variable_name: str, module_name: str, mapping: dict
) -> None:
    msg = (
        f"Variable '{variable_name}' found in multiple modules:"
        f"'{mapping[variable_name]['module']}' and '{module_name}'."
        f" Each variable must be unique to a single module."
        f" Either combine the variable from both modules inside a script"
        f" or rename the variable in one of the cleaning scripts of the modules."
    )
    raise ValueError(msg)


def _create_variable_metadata(
    map_modules_to_variables_and_their_metadata: dict,
) -> dict[str, dict]:
    """Return a mapping of variables to their metadata.

    Args:
        map_modules_to_variables_and_their_metadata: Map of module to variables
             and their metadata information.

    Returns:
        A mapping of variable to metadata.
    """
    mapping = {}
    for (
        module_name,
        variables_to_metadata,
    ) in map_modules_to_variables_and_their_metadata.items():
        for name, metadata in variables_to_metadata["variable_metadata"].items():
            if name not in mapping:
                mapping[name] = {"module": module_name} | metadata
            else:
                _fail_if_variable_in_multiple_modules(
                    variable_name=name,
                    module_name=module_name,
                    mapping=mapping,
                )
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


def task_create_variable_metadata(
    modules_metadata: Annotated[
        dict[str, pd.DataFrame], DATA_CATALOGS["metadata"]._entries
    ],
    in_path: Annotated[
        Path, SRC / "create_metadata" / "variable_to_metadata_mapping.yaml"
    ],
    out_path: Annotated[Path, Product] = BLD / "variable_to_metadata_mapping.yaml",
) -> None:
    """Create a mapping of variables to metadata and store as YAML file.

    Args:
        modules_metadata: Map of module to metadata information.
        in_path: The path to compare the output against.
        out_path: The path to the YAML file to write.

    Raises:
        TypeError: If input data or data name is not of expected type.
    """
    _error_handling_mapping_task(modules_metadata)
    mapping = _create_variable_metadata(modules_metadata)
    with Path.open(in_path, "r", encoding="utf-8") as file:
        existing_mapping = yaml.safe_load(file)
    if mapping != existing_mapping:
        msg = (
            f"The generated variable to metadata mapping differs from the existing"
            f" mapping at {in_path}."
            f" Please review the changes before proceeding."
        )
        raise ValueError(msg)
    with Path.open(out_path, "w", encoding="utf-8") as file:
        yaml.dump(mapping, file, encoding="utf-8", allow_unicode=True)


def _error_handling_mapping_task(modules_metadata: Any) -> None:
    fail_if_input_has_invalid_type(input_=modules_metadata, expected_dtypes=["dict"])
    fail_if_empty(input_=modules_metadata, name="modules_metadata")
    for metadata_name, metadata in modules_metadata.items():
        fail_if_input_has_invalid_type(input_=metadata_name, expected_dtypes=["str"])
        fail_if_input_has_invalid_type(input_=metadata, expected_dtypes=["dict"])
