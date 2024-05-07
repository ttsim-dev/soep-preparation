"""Utilities used in various parts of the project."""

import pandas as pd


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
