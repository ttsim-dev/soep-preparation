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


def _fail_if_mapping_changed(  # noqa: C901
    new_mapping: dict[str, Any],
    existing_mapping: dict[str, Any],
    new_mapping_path: Path,
) -> None:
    # Collect error messages for all variables with issues
    variables_with_errors: dict[str, dict[str, Any]] = {}

    for variable, metadata in new_mapping.items():
        module_name = metadata["module"]
        script_dir = "combine_modules" if "_" in module_name else "clean_modules"

        error_messages = []
        is_new_variable = variable not in existing_mapping

        if is_new_variable:
            error_messages.append(
                "  - added: present in new mapping, but not in existing mapping"
            )
        elif metadata != existing_mapping[variable]:
            existing_metadata = existing_mapping[variable]

            if metadata["dtype"] != existing_metadata["dtype"]:
                error_messages.append(
                    f"  - dtype changed from {existing_metadata['dtype']} "
                    f"to {metadata['dtype']}"
                )

            if metadata["survey_years"] != existing_metadata["survey_years"]:
                old_years = set(existing_metadata["survey_years"] or [])
                new_years = set(metadata["survey_years"] or [])
                added_years = sorted(new_years - old_years)
                removed_years = sorted(old_years - new_years)
                if added_years:
                    error_messages.append(f"  - new survey years: {added_years}")
                if removed_years:
                    error_messages.append(f"  - removed survey years: {removed_years}")

        if error_messages:
            variables_with_errors[variable] = {
                "script_path": SRC / script_dir / f"{module_name}.py",
                "error_messages": error_messages,
            }

    for variable, existing_metadata in existing_mapping.items():
        if variable not in new_mapping:
            module_name = existing_metadata["module"]
            script_dir = "combine_modules" if "_" in module_name else "clean_modules"
            variables_with_errors[variable] = {
                "script_path": SRC / script_dir / f"{module_name}.py",
                "error_messages": [
                    "  - removed: present in existing mapping, but not in new mapping"
                ],
            }

    if variables_with_errors:
        existing_mapping_path = (
            SRC / "create_metadata" / "variable_to_metadata_mapping.yaml"
        ).resolve()
        intro = (
            "The newly generated mapping of variables to their metadata differs"
            " from the existing mapping. Differences:\n\n"
        )

        variable_messages = []
        for variable, info in variables_with_errors.items():
            msg_lines = [f"{variable}: {info['script_path']}"] + info["error_messages"]
            variable_messages.append("\n".join(msg_lines))

        variables_msg = "\n\n".join(variable_messages) + "\n\n"

        copy_mapping_msg = (
            f"If the changes are intentional, please update the mapping file at:\n"
            f"{existing_mapping_path}\n"
            f"by copying over the newly generated mapping from:\n"
            f"{new_mapping_path.resolve()}\n"
            f"and run pytask again.\n\n"
            f"To copy the mapping file, please run the following command:\n"
            f"cp {new_mapping_path.resolve()} {existing_mapping_path}\n"
        )
        raise ValueError(intro + variables_msg + copy_mapping_msg)
