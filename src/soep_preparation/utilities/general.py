"""General utility functions."""

import inspect
import re
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType


def _fail_if_no_raw_soep_module_exists(data_root: Path, soep_version: str) -> None:
    raw_data_path = data_root / soep_version
    if not any(raw_data_path.glob("*.dta")):
        msg = (
            f"No raw SOEP data files found in {raw_data_path} for SOEP {soep_version}."
            f"Please add at least one raw data file to the data directory under"
            f" the specified SOEP version and create a cleaning script for it."
            f" For further instructions, please refer to the documentation at https://github.com/ttsim-dev/soep-preparation?tab=readme-ov-file#usage"
        )
        raise FileNotFoundError(msg)


def _fail_if_raw_soep_module_missing(
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
    _fail_if_no_raw_soep_module_exists(data_root, soep_version)
    for script_name in script_names:
        _fail_if_raw_soep_module_missing(script_name, data_root, soep_version)
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
        for raw_module in combine_module.split("_"):
            if raw_module not in get_script_names(
                directory=directory.parent / "clean_modules"
            ):
                break
        else:
            combine_module_names.append(combine_module)
    return combine_module_names


def get_relevant_column_names(script_path: Path) -> list[str]:
    """Get relevant column names from the cleaning script.

    Args:
        script_path: The path to the cleaning script.

    Returns:
        A list of relevant column names.
    """
    script = load_script(script_path)
    # Remove the docstring, if existent.
    function_source = re.sub(
        r'""".*?"""|\'\'\'.*?\'\'\'',
        "",
        inspect.getsource(script.clean),
        flags=re.DOTALL,
    )
    # Find all occurrences of raw["column_name"] or ['column_name'].
    pattern = r'raw_data\["([^"]+)"\]|\[\'([^\']+)\'\]'
    matches = [match[0] or match[1] for match in re.findall(pattern, function_source)]
    return list(dict.fromkeys(matches))


def load_script(script_path: Path) -> ModuleType:
    """Load script from path.

    Args:
        script_path: The path to the script.

    Returns:
        The loaded script.
    """
    script_name = script_path.stem
    spec = spec_from_file_location(name=script_name, location=script_path)
    script = module_from_spec(spec)
    spec.loader.exec_module(script)
    return script
