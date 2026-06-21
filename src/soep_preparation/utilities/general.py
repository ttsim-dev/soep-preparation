"""General utility functions."""

import ast
import inspect
import textwrap
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from importlib.abc import Loader
    from importlib.machinery import ModuleSpec


def _fail_if_raw_data_files_are_missing(
    data_root: Path, soep_version: str, script_names: list[str]
) -> None:
    missing_files = []
    raw_data_dir = data_root / soep_version
    for script_name in script_names:
        raw_data_file_path = raw_data_dir / f"{script_name}.dta"
        if not raw_data_file_path.exists():
            missing_files.append(raw_data_file_path)
    if missing_files:
        missing_files_str = "\n".join(str(file) for file in missing_files)
        msg = (
            f"The following raw data files are missing for SOEP {soep_version}:\n\n"
            f"{missing_files_str}\n\n"
            "Please ensure these files are present in the data directory\n\n"
            f"{raw_data_dir}\n\n"
            f"Also see the documentation at https://github.com/ttsim-dev/soep-preparation?tab=readme-ov-file#usage"
        )
        raise FileNotFoundError(msg)


def get_script_names(directory: Path) -> list[str]:
    """Get the names of all scripts in the given directory.

    Args:
        directory: The directory containing scripts.

    Returns:
        A list of script names.
    """
    return [
        script.stem
        for script in directory.glob("*.py")
        if script.name not in ["__init__.py", "task.py"]
    ]


def get_raw_data_file_names(
    directory: Path, data_root: Path, soep_version: str
) -> list[str]:
    """Get names of all scripts in the directory with corresponding raw data files.

    Args:
        directory: The directory containing scripts.
        data_root: The root directory where data files are stored.
        soep_version: The version of the SOEP data.

    Returns:
        A list of data file names.

    """
    script_names = get_script_names(directory)
    _fail_if_raw_data_files_are_missing(
        data_root=data_root,
        soep_version=soep_version,
        script_names=script_names,
    )
    return script_names


def get_combine_module_names(directory: Path) -> list[str]:
    """Get names of all combine scripts in the directory with valid raw modules.

    Args:
        directory: The directory containing combine scripts.

    Returns:
        A list of combine module names.

    """
    combine_module_names = []
    script_names = get_script_names(directory)
    for combine_module in script_names:
        for orig_module in combine_module.split("_"):
            clean_module_directory = directory.parent / "clean_modules"
            if orig_module not in get_script_names(clean_module_directory):
                msg = (
                    f"No cleaning script for module {orig_module}"
                    f" found in clean modules at {clean_module_directory}."
                    f"Existence is implied by combine script {combine_module}."
                )
                raise ValueError(msg)
        combine_module_names.append(combine_module)
    return combine_module_names


def get_relevant_column_names(script_path: Path) -> list[str]:
    """Get relevant column names from the cleaning script.

    Args:
        script_path: The path to the cleaning script.

    Returns:
        A list of relevant column names.
    """
    script = load_script(script_path, expected_function="clean")
    tree = ast.parse(textwrap.dedent(inspect.getsource(script.clean)))
    # Collect `raw_data["column"]` subscripts with a string-literal key. Parsing
    # the AST (rather than scanning text) ignores the same name appearing in
    # comments, docstrings, or dynamic f-string keys, none of which are real
    # column reads.
    columns_in_source_order = sorted(
        (node.lineno, node.col_offset, node.slice.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Subscript)
        and isinstance(node.value, ast.Name)
        and node.value.id == "raw_data"
        and isinstance(node.slice, ast.Constant)
        and isinstance(node.slice.value, str)
        and node.slice.value
    )
    return list(dict.fromkeys(column for _, _, column in columns_in_source_order))


def load_script(script_path: Path, expected_function: str) -> ModuleType:
    """Load script from path and verify it contains the expected function.

    Args:
        script_path: The path to the script.
        expected_function: The expected function name that should exist in the script.

    Returns:
        The loaded script.

    Raises:
        AttributeError: If expected function is missing in script.
    """
    script_name = script_path.stem
    spec = spec_from_file_location(name=script_name, location=script_path)
    spec = cast("ModuleSpec", spec)
    script = module_from_spec(spec)
    loader = cast("Loader", spec.loader)
    loader.exec_module(script)

    if not hasattr(script, expected_function):
        msg = (
            f"The script at {script_path}"
            f" does not contain the expected function {expected_function}."
        )
        raise AttributeError(msg)

    return script
