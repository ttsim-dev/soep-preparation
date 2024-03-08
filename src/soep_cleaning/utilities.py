"""Utilities used in various parts of the project."""

import yaml
import pandas as pd


def read_yaml(path):
    """Read a YAML file.

    Args:
        path (str or pathlib.Path): Path to file.

    Returns:
        dict: The parsed YAML file.

    """
    with open(path) as stream:
        try:
            out = yaml.safe_load(stream)
        except yaml.YAMLError as error:
            info = (
                "The YAML file could not be loaded. Please check that the path points "
                "to a valid YAML file."
            )
            raise ValueError(info) from error
    return out

def find_lowest_int_dtype(sr: pd.Series):
    """Find the lowest integer dtype for a series.

    Args:
        sr (pd.Series): The series to check.

    Returns:
        str: The lowest integer dtype.

    """
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