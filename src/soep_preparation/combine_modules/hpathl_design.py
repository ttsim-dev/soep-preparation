"""Combine variables from the modules hpathl and design."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    combine_first_and_make_categorical,
)


def combine(hpathl: pd.DataFrame, design: pd.DataFrame) -> pd.DataFrame:
    """Combine variables from the cleaned hpathl and design modules.

    Args:
        hpathl: Cleaned hpathl module.
        design: Cleaned design module.

    Returns:
        Combined hpathl and design modules.
    """
    out = pd.DataFrame(index=hpathl.index)
    merged = pd.merge(hpathl, design, on=["hh_id"], how="outer")
    out["hh_id"] = hpathl["hh_id"].copy()
    out["hh_soep_sample"] = combine_first_and_make_categorical(
        series_1=merged["hh_soep_sample_hpathl"],
        series_2=merged["hh_soep_sample_design"],
        ordered=False,
    )
    return out
