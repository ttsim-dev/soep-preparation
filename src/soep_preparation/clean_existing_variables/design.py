"""Clean and convert SOEP design variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_smallest_int_dtype,
    object_to_str_categorical,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the design data file.

    Args:
        raw_data: The raw design data.

    Returns:
        The processed design data.
    """
    out = pd.DataFrame()
    out["hh_id"] = apply_smallest_int_dtype(raw_data["cid"])

    out["hh_random_group"] = apply_smallest_int_dtype(raw_data["rgroup"])
    out["hh_strat"] = apply_smallest_int_dtype(raw_data["strat"])
    out["hh_soep_sample_design"] = object_to_str_categorical(
        raw_data["hsample"],
        nr_identifiers=2,
    )
    return out
