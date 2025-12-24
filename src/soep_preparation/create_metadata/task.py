"""Tasks to create metadata."""

from pathlib import Path
from typing import Annotated, Any

import pandas as pd
import yaml
from deepdiff import DeepHash
from pytask import DataCatalog, Product, PythonNode, task

from soep_preparation.config import (
    BLD,
    METADATA,
    MODULES,
    POTENTIAL_INDEX_VARIABLES,
    SRC,
)

_METADATA_CATALOG = DataCatalog(name="metadata")


def _calculate_hash(x: Any) -> int | str:
    return DeepHash(x)[x]


for module_name in MODULES._entries:  # noqa: SLF001

    @task(id=module_name)
    def task_create_metadata_for_one_module(
        module: Annotated[pd.DataFrame, MODULES[module_name]],
    ) -> Annotated[dict, _METADATA_CATALOG[module_name]]:
        """Create metadata for a single module.

        Args:
            module: The data module to create metadata for.

        Returns:
            Metadata information for index and variables contained in the module.

        Raises:
            TypeError: If input data is not of expected type.
        """
        index_variables_metadata = _get_index_variables_metadata(module)
        variable_metadata = _get_variable_metadata(module)
        return {
            "index_variables": index_variables_metadata,
            "variable_metadata": variable_metadata,
        }


@task(id="create_metadata")
def task_create_variable_to_metadata_mapping_yaml(
    modules_metadata: Annotated[dict[str, dict], _METADATA_CATALOG._entries],
    current_metadata: Annotated[
        dict[str, Any], PythonNode(value=METADATA, hash=_calculate_hash)
    ],
    out_path: Annotated[Path, Product] = BLD / "variable_to_metadata_mapping.yaml",
) -> None:
    """Create a mapping of variables to metadata and store as YAML file.

    Args:
        modules_metadata: Map of module to metadata information.
        current_metadata: The current metadata to compare the output against.
        out_path: The path to the YAML file to write.

    Raises:
        TypeError: If input data or data name is not of expected type.
    """
    new_metadata = _create_variable_metadata(modules_metadata)

    with out_path.open("w", encoding="utf-8") as file:
        yaml.dump(
            data=new_metadata,
            stream=file,
            width=60,  # Big differences how Python / yamllint count, leave buffer.
            default_flow_style=False,
            encoding="utf-8",
            allow_unicode=True,
            explicit_start=True,
        )
    if new_metadata != current_metadata:
        _fail_if_mapping_changed(
            new_mapping=new_metadata,
            existing_mapping=current_metadata,
            new_mapping_path=out_path,
        )


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


def _serialize_categorical_dtype(variable_dtype: pd.CategoricalDtype) -> dict:
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
            serialized_variable_dtype = {
                "categorical": _serialize_categorical_dtype(variable_dtype)
            }
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


def _fail_if_mapping_changed(  # noqa: C901, PLR0912
    new_mapping: dict[str, Any],
    existing_mapping: dict[str, Any],
    new_mapping_path: Path,
) -> None:
    variables_to_warn_about = {
        "new_variables": {},
        "changed_dtypes": {},
        "changed_survey_years": {},
    }

    for variable, metadata in new_mapping.items():
        module_name = metadata["module"]
        if "_" in module_name:
            corresponding_script_path = SRC / "combine_modules" / f"{module_name}.py"
        else:
            corresponding_script_path = SRC / "clean_modules" / f"{module_name}.py"
        if variable not in existing_mapping:
            variables_to_warn_about["new_variables"][variable] = {
                "metadata": metadata,
                "module_name": module_name,
                "script_path": corresponding_script_path,
            }
        elif metadata != existing_mapping[variable]:
            if metadata["dtype"] != existing_mapping[variable]["dtype"]:
                variables_to_warn_about["changed_dtypes"][variable] = {
                    "metadata": metadata,
                    "module_name": module_name,
                    "script_path": corresponding_script_path,
                }
            if metadata["survey_years"] != existing_mapping[variable]["survey_years"]:
                variables_to_warn_about["changed_survey_years"][variable] = {
                    "metadata": metadata,
                    "module_name": module_name,
                    "script_path": corresponding_script_path,
                }

    # if any changes, raise error with specific message
    if any(variables_to_warn_about.values()):
        existing_mapping_path = (
            SRC / "create_metadata" / "variable_to_metadata_mapping.yaml"
        ).resolve()
        intro = (
            f"The newly generated mapping of variables to their metadata differs"
            f" from the existing mapping at:\n{existing_mapping_path}.\n\n"
        )
        new_variable_msg = ""
        dtype_change_msg = ""
        survey_years_change_msg = ""

        if variables_to_warn_about["new_variables"]:
            for variable, info in variables_to_warn_about["new_variables"].items():
                if len(new_variable_msg) == 0:
                    new_variable_msg += "New variables were added:\n"
                new_variable_msg += (
                    f"{variable} with metadata {info['metadata']} was added."
                    f" If adding the variable was not intended,"
                    f" remove the variable from the script creating module"
                    f" {info['module_name']} at {info['script_path']}.\n\n"
                )

        if variables_to_warn_about["changed_dtypes"]:
            for variable, info in variables_to_warn_about["changed_dtypes"].items():
                if len(dtype_change_msg) == 0:
                    dtype_change_msg += "Variables dtypes changed in the metadata:\n"
                dtype_change_msg += (
                    f"Dtype for variable {variable} changed from\n"
                    f"{existing_mapping[variable]['dtype']} to\n"
                    f"{info['metadata']['dtype']}.\n"
                    f"If the change was not intended, inspect changes to the script"
                    f" creating module {info['module_name']} at"
                    f" {info['script_path']}, to ensure dtype consistency.\n\n"
                )
        if variables_to_warn_about["changed_survey_years"]:
            for variable, info in variables_to_warn_about[
                "changed_survey_years"
            ].items():
                if len(survey_years_change_msg) == 0:
                    survey_years_change_msg += (
                        "Variables observed survey years changed in the metadata:\n"
                    )
                survey_years_change_msg += (
                    f"The survey years observed changed for variable {variable} from\n"
                    f"{existing_mapping[variable]['survey_years']} to\n"
                    f"{info['metadata']['survey_years']}.\n"
                    f"If the change was not intended, inspect changes to the script"
                    f" creating module {info['module_name']} at"
                    f" {info['script_path']}, to ensure survey years consistency.\n\n"
                )

        copy_mapping_msg = (
            f"If the changes are intentional, please update the mapping file at:\n"
            f"{existing_mapping_path}\n"
            f"by copying over the newly generated mapping from:\n"
            f"{new_mapping_path.resolve()}\n"
            f"and run pytask again.\n\n"
            f"To copy the mapping file, please run the following command:\n"
            f"cp {new_mapping_path.resolve()} {existing_mapping_path}\n"
        )
        raise ValueError(
            intro
            + new_variable_msg
            + dtype_change_msg
            + survey_years_change_msg
            + copy_mapping_msg
        )
