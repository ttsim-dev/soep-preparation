"""Utilities used in various parts of the project."""
from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Annotated

from soep_cleaning.config import pd


def list_scripts(directory: Annotated[Path, Path], scripts_kind: str) -> list:
    """List the scripts in a directory.

    Args:
        directory (Path): The directory to search.
        scripts_kind (str): The kind of scripts to search for.

    Returns:
        list: A list of the scripts.

    """
    return list(directory.glob(f"*{scripts_kind}.py"))


def list_functions_in_scripts(
    directory: Annotated[Path, Path],
    scripts_kind: str,
) -> list:
    list_of_scripts = list_scripts(directory, scripts_kind)
    list_of_modules = [
        dir(
            SourceFileLoader(
                script.resolve().stem,
                str(script.resolve()),
            ).load_module(),
        )
        for script in list_of_scripts
    ]
    return [
        module
        for modules in list_of_modules
        for module in modules
        if ("_" not in module) and ("pd" not in module)
    ]


def dataset_script_name(dataset_name: str) -> str:
    """Map the dataset name to the name of the respective script."""
    if dataset_name.startswith("bio"):
        return "bio_specific"
    elif dataset_name.startswith("h"):
        return "h_specific"
    elif dataset_name.startswith("p"):
        return "p_specific"
    else:
        return "other_specific"


def find_lowest_int_dtype(sr: pd.Series) -> str:
    """Find the lowest integer dtype for a series.

    Args:
        sr (pd.Series): The series to check.

    Returns:
        str: The lowest integer dtype.

    """
    if "float" in sr.dtype.name:
        sr = sr.astype("float[pyarrow]")
    if sr.min() >= 0:
        if sr.max() <= 255:
            return "uint8[pyarrow]"
        if sr.max() <= 65535:
            return "uint16[pyarrow]"
        if sr.max() <= 4294967295:
            return "uint32[pyarrow]"
        return "uint64[pyarrow]"
    if sr.min() >= -128 and sr.max() <= 127:
        return "int8[pyarrow]"
    if sr.min() >= -32768 and sr.max() <= 32767:
        return "int16[pyarrow]"
    if sr.min() >= -2147483648 and sr.max() <= 2147483647:
        return "int32[pyarrow]"
    return "int64[pyarrow]"


def apply_lowest_int_dtype(sr: pd.Series, remove_negatives: bool = False) -> str:
    """Apply the lowest integer dtype to a series."""
    if remove_negatives:
        sr[sr < 0] = pd.NA
    return sr.astype(find_lowest_int_dtype(sr))


def find_lowest_float_dtype(sr: pd.Series) -> str:
    """Find the lowest float dtype for a series.

    Args:
        sr (pd.Series): The series to check.

    Returns:
        str: The lowest float dtype.

    """
    if sr.isna().any():
        return "float64[pyarrow]"
    if sr.min() >= 0:
        if sr.max() <= 3.4028235e38:
            return "float32[pyarrow]"
        return "float64[pyarrow]"
    if sr.min() >= -1.7976931348623157e308 and sr.max() <= 1.7976931348623157e308:
        return "float64[pyarrow]"
    return "float64[pyarrow]"


def apply_lowest_float_dtype(sr: pd.Series, remove_negatives: bool = False) -> str:
    """Apply the lowest integer dtype to a series."""
    if remove_negatives:
        sr[sr < 0] = pd.NA
    return sr.astype(find_lowest_float_dtype(sr))
