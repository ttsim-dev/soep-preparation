"""Clean and convert SOEP bioparen variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_lowest_int_dtype,
    float_to_int,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the biobirth file.

    Args:
        raw_data (pd.DataFrame): The raw biobirth data.

    Returns:
        pd.DataFrame: The processed biobirth data.
    """
    out = pd.DataFrame()
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])

    out["p_id_father"] = float_to_int(
        raw_data["fnr"],
        drop_missing=True,
    )  # social father personal id
    out["p_id_mother"] = float_to_int(
        raw_data["mnr"],
        drop_missing=True,
    )  # social mother personal id
    return out
