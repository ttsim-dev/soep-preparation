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
        Combined pl and pkal modules. If contents conflict with each other,
             the one from pl takes precedence.
    """
    merged = pd.merge(
        left=pl, right=pkal, on=["p_id", "hh_id", "survey_year"], how="outer"
    )
    out = pd.DataFrame(index=merged.index)
    out["p_id"] = merged["p_id"]
    out["hh_id"] = merged["hh_id"]
    out["survey_year"] = merged["survey_year"]

    out["bezog_mutterschaftsgeld"] = combine_first_and_make_categorical(
        series_1=merged["bezog_mutterschaftsgeld_pl"],
        series_2=merged["bezog_mutterschaftsgeld_pkal"],
        ordered=False,
    )
    return out
