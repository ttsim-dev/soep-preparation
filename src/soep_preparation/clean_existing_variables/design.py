"""Functions to pre-process variables for a raw design dataset.

Functions:
- clean: Coordinates the pre-processing for the dataset.

Usage:
    Import this module and call clean to pre-process variables.
"""

import pandas as pd

from soep_preparation.utilities import (
    apply_lowest_int_dtype,
    str_categorical,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["hh_id"] = apply_lowest_int_dtype(raw_data["cid"])
    out["hh_random_group"] = apply_lowest_int_dtype(raw_data["rgroup"])
    out["hh_strat"] = apply_lowest_int_dtype(raw_data["strat"])

    out["hh_soep_sample_from_design"] = str_categorical(
        raw_data["hsample"],
        nr_identifiers=2,
    )

    return out
