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
        Combined hpathl and design modules. If contents conflict with each other,
             the one from hpathl takes precedence.
    """
    merged = pd.merge(left=hpathl, right=design, on=["hh_id"], how="outer")
    out = pd.DataFrame(index=merged.index)
    out["hh_id"] = merged["hh_id"]
    out["hh_soep_sample"] = combine_first_and_make_categorical(
        series_1=merged["hh_soep_sample_hpathl"],
        series_2=merged["hh_soep_sample_design"],
        ordered=False,
    )
    return out
