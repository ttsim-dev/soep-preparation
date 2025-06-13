"""Clean and convert SOEP bioedu variables to appropriate data types."""

import pandas as pd

from soep_preparation.clean_existing_variables import month_mapping
from soep_preparation.utilities.series_manipulator import (
    apply_lowest_int_dtype,
    object_to_int_categorical,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the bioedu file.

    Args:
        raw_data: The raw bioedu data.

    Returns:
        The processed bioedu data.
    """
    out = pd.DataFrame()
    out["hh_id_original"] = apply_lowest_int_dtype(raw_data["cid"])
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])

    out["birth_month_bioedu"] = object_to_int_categorical(
        raw_data["gebmonat"],
        renaming=month_mapping.en,
        ordered=True,
    )
    return out
