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
        file.stem
        for file in directory.glob("*.py")
        if file.name not in ["__init__.py", "task.py"]
    ]


def get_stems_if_corresponding_raw_file_exists(directory: Path) -> list[str]:
    """Get the names of all scripts in the given directory with corresponding raw files.

    Args:
        directory: The directory containing scripts.

    Returns:
        A list of file names.

    """
    from soep_preparation.config import DATA, SOEP_VERSION

    return [
        file.stem
        for file in directory.glob("*.py")
        if file.name not in ["__init__.py", "task.py"]
        and (DATA / f"{SOEP_VERSION}" / f"{file.stem}.dta").exists()
    ]


def load_module(script_path: Path) -> ModuleType:
    """Load module from path.

    Args:
        script_path: The path to the script.

    Returns:
        The loaded module.
    """
    module_name = script_path.stem
    spec = spec_from_file_location(module_name, script_path)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
