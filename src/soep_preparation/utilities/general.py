"""General utility functions."""

import inspect
import re
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType
from typing import Any


def _fail_if_raw_data_file_missing(
    script_name: str, data_root: Path, soep_version: str
) -> None:
    raw_data_file_path = data_root / f"{soep_version}" / f"{script_name}.dta"
    if not raw_data_file_path.exists():
        msg = (
            f"Raw data file {raw_data_file_path} not found for SOEP {soep_version}.\n"
            f"Ensure the file is present in the data directory\n"
            f" corresponding to the specified wave."
        )
        raise FileNotFoundError(msg)


def get_variable_names_in_module(module: Any) -> list[str]:  # noqa: ANN401
    """Get the variable names in the module.

    Args:
        module: The module to get the variable names from.

    Returns:
        The variable names in the module.
    """
    return [
        variable_name.split("derive_")[-1]
        for variable_name in module.__dict__
        if variable_name.startswith("derive_")
    ]


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


def get_data_file_names(
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
    for script_name in script_names:
        _fail_if_raw_data_file_missing(script_name, data_root, soep_version)
    return script_names


def get_relevant_column_names(script_path: Path) -> list[str]:
    """Get relevant column names from the cleaning script.

    Args:
        script_path: The path to the cleaning script.

    Returns:
        A list of relevant column names.
    """
    module = load_module(script_path)
    # Remove the docstring, if existent.
    function_source = re.sub(
        r'""".*?"""|\'\'\'.*?\'\'\'',
        "",
        inspect.getsource(module.clean),
        flags=re.DOTALL,
    )
    # Find all occurrences of raw["column_name"] or ['column_name'].
    pattern = r'raw_data\["([^"]+)"\]|\[\'([^\']+)\'\]'
    matches = [match[0] or match[1] for match in re.findall(pattern, function_source)]
    return list(dict.fromkeys(matches))


def load_module(script_path: Path) -> ModuleType:
    """Load module from path.

    Args:
        script_path: The path to the script.

    Returns:
        The loaded module.
    """
    module_name = script_path.stem
    spec = spec_from_file_location(name=module_name, location=script_path)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
