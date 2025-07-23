"""General utility functions."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType


def _fail_if_raw_data_file_missing(
    script_name: str, data_root: Path, soep_version: str
) -> None:
    raw_data_file_path = data_root / f"{soep_version}" / f"{script_name}.dta"
    if not raw_data_file_path.exists():
        msg = f"""Raw data file {raw_data_file_path} not found for SOEP {soep_version}.
        Ensure the file is present in the data directory for the corresponding wave."""
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
