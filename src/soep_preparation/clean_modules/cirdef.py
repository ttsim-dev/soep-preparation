"""Clean and convert SOEP cirdef variables to appropriate data types.

The cirdef file documents household-level random-group / sample membership.
Its main purpose here is to flag households that belong to the **SOEP
teaching sample**: random groups 11 through 20 of ``rgroup20``.
"""

import pandas as pd

from soep_preparation.utilities.data_manipulator import apply_smallest_int_dtype


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned variables from the cirdef module.

    Args:
        raw_data: The raw cirdef data.

    Returns:
        The processed cirdef data.
    """
    out = pd.DataFrame()
    out["hh_id_original"] = apply_smallest_int_dtype(raw_data["cid"])
    out["rgroup20"] = apply_smallest_int_dtype(raw_data["rgroup20"])
    out["teaching_sample"] = (out["rgroup20"].between(11, 20)).astype("bool[pyarrow]")
    return out
