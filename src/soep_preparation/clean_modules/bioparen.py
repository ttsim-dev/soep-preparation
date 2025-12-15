"""Clean and convert SOEP bioparen variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    apply_smallest_int_dtype,
    float_to_int,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned variables from the biobirth module.

    Args:
        raw_data: The raw biobirth data.

    Returns:
        The processed biobirth data.
    """
    out = pd.DataFrame()
    out["p_id"] = apply_smallest_int_dtype(raw_data["pid"])
    out["hh_id_original"] = apply_smallest_int_dtype(raw_data["cid"])

    out["p_id_father_1"] = float_to_int(
        series=raw_data["fnr1"],
        drop_missing=True,
    )
    out["p_id_father_2"] = float_to_int(
        series=raw_data["fnr2"],
        drop_missing=True,
    )

    out["p_id_mother_1"] = float_to_int(
        series=raw_data["mnr1"],
        drop_missing=True,
    )
    out["p_id_mother_2"] = float_to_int(
        series=raw_data["mnr2"],
        drop_missing=True,
    )
    return out
