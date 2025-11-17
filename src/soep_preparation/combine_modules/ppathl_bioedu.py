"""Combine variables from the modules ppathl and bioedu."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    combine_first_and_make_categorical,
)


def combine(ppathl: pd.DataFrame, bioedu: pd.DataFrame) -> pd.DataFrame:
    """Combine variables from the cleaned ppathl and bioedu modules.

    Args:
        ppathl: Cleaned ppathl module.
        bioedu: Cleaned bioedu module.

    Returns:
        Combined ppathl and bioedu modules.
    """
    out = pd.DataFrame()
    merged = pd.merge(left=ppathl, right=bioedu, on="p_id", how="outer")
    out["p_id"] = merged["p_id"].unique()
    out["birth_month"] = combine_first_and_make_categorical(
        series_1=merged["birth_month_ppathl"],
        series_2=merged["birth_month_bioedu"],
        ordered=False,
    )
    return out
