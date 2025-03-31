"""Functions to pre-process variables for a raw bioparen dataset.

Functions:
- clean: Coordinates the pre-processing for the dataset.

Usage:
    Import this module and call clean to pre-process variables.
"""

import pandas as pd

from soep_preparation.utilities import apply_lowest_int_dtype, float_categorical_to_int


def clean(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the biobirth dataset."""
    out = pd.DataFrame()
    out["p_id"] = apply_lowest_int_dtype(raw["pid"])
    out["p_id_father"] = float_categorical_to_int(
        raw["fnr"]
    )  # social father personal id
    out["p_id_mother"] = float_categorical_to_int(
        raw["mnr"]
    )  # social mother personal id
    return out
