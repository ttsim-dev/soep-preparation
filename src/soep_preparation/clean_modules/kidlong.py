"""Clean and convert SOEP kidlong variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    apply_smallest_int_dtype,
    object_to_int,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned variables from the kidlong module.

    Args:
        raw_data: The raw kidlong data.

    Returns:
        The processed kidlong data.
    """
    out = pd.DataFrame()
    out["hh_id"] = apply_smallest_int_dtype(raw_data["hid"])
    out["p_id"] = apply_smallest_int_dtype(raw_data["pid"])
    out["survey_year"] = apply_smallest_int_dtype(raw_data["syear"])

    out["pointer_mother"] = object_to_int(raw_data["k_pmum"])

    return out
