"""Functions to pre-process variables for a raw bioedu dataset."""

import pandas as pd

from soep_preparation.clean_existing_variables import month_mapping
from soep_preparation.utilities.series_manipulator import (
    apply_lowest_int_dtype,
    object_to_int_categorical,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the bioedu dataset."""
    out = pd.DataFrame()
    out["hh_id_orig"] = apply_lowest_int_dtype(raw_data["cid"])
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])
    out["birth_month_from_bioedu"] = object_to_int_categorical(
        raw_data["gebmonat"],
        renaming=month_mapping.en,
        ordered=True,
    )
    return out
