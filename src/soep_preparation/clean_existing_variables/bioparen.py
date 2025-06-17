"""Clean and convert SOEP bioparen variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_smallest_int_dtype,
    float_to_int,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the biobirth file.

    Args:
        raw_data: The raw biobirth data.

    Returns:
        The processed biobirth data.
    """
    out = pd.DataFrame()
    out["p_id"] = apply_smallest_int_dtype(raw_data["pid"])

    # Social father personal id
    out["p_id_father"] = float_to_int(
        raw_data["fnr"],
        drop_missing=True,
    )

    # Social mother personal id
    out["p_id_mother"] = float_to_int(
        raw_data["mnr"],
        drop_missing=True,
    )
    return out
