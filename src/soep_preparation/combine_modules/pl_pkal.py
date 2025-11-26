"""Combine variables from the modules pl and pkal."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    combine_first_and_make_categorical,
)


def combine(pl: pd.DataFrame, pkal: pd.DataFrame) -> pd.DataFrame:
    """Combine variables from the cleaned pl and pkal modules.

    Args:
        pl: Cleaned pl module.
        pkal: Cleaned pkal module.

    Returns:
        Combined pl and pkal modules.
    """
    merged = pd.merge(left=pl, right=pkal, on=["hh_id", "survey_year"], how="outer")
    out = pd.DataFrame(index=merged.index)
    out[["p_id", "hh_id", "survey_year"]] = pl[["p_id", "hh_id", "survey_year"]].copy()
    out["bezog_mutterschaftsgeld"] = combine_first_and_make_categorical(
        series_1=merged["bezog_mutterschaftsgeld_pl"],
        series_2=merged["bezog_mutterschaftsgeld_pkal"],
        ordered=False,
    )
    return out
