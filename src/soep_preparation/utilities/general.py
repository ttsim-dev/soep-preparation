"""General utility functions."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType


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


def get_stems_if_corresponding_raw_data_file_exists(directory: Path) -> list[str]:
    """Get names of all scripts in the directory with corresponding raw data files.

    Args:
        directory: The directory containing scripts.

    Returns:
        A list of data file names.

    """
    from soep_preparation.config import DATA, SOEP_VERSION

    return [
        script.stem
        for script in directory.glob("*.py")
        if (DATA / f"{SOEP_VERSION}" / f"{script.stem}.dta").exists()
    ]


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
