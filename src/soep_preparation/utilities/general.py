"""General utility functions."""

import inspect
import re
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType


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
    script = load_script(script_path, expected_function="clean")
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
    script = module_from_spec(spec)
    spec.loader.exec_module(script)

    if not hasattr(script, expected_function):
        msg = (
            f"The script at {script_path}"
            f" does not contain the expected function {expected_function}."
        )
        raise AttributeError(msg)

    return script
