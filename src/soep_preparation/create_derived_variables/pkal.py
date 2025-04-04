"""Functions to create variables for a pre-processed pkal dataset.

Functions:
- manipulate: Coordinates the variable creation process for the dataset.

Usage:
    Import this module and call manipulate to generate new variables.
"""

import pandas as pd
from pandas.api.types import union_categoricals

from soep_preparation.utilities import apply_lowest_int_dtype


def _full_empl_prev_m(
    v1_prev_m: "pd.Series[pd.Categorical]",
    v2_prev_m: "pd.Series[pd.Categorical]",
    survey_year: "pd.Series[int]",
) -> "pd.Series[pd.Categorical]":
    v1_prev_m_aligned = v1_prev_m.cat.set_categories(v2_prev_m.cat.categories)
    return pd.Series(
        union_categoricals(
            [
                v1_prev_m_aligned.where(survey_year < 1997, v2_prev_m),  # noqa: PLR2004
                v2_prev_m,
            ],
        ),
    )


def _empl_m_prev(
    full: "pd.Series[pd.Categorical]",
    half: "pd.Series[pd.Categorical]",
    mini_job: "pd.Series[pd.Categorical]",
) -> "pd.Series[int]":
    out = pd.Series(pd.NA, index=full.index)
    out = out.where((full != 1) | ~(half) | (mini_job != 1), 1)
    return apply_lowest_int_dtype(out)


def create_derived_variables(data: pd.DataFrame) -> pd.DataFrame:
    """Create derived variables for the pkal dataset.

    Args:
        data (pd.DataFrame): The dataset required.

    Returns:
        pd.DataFrame: The dataset of derived variables.
    """
    out = pd.DataFrame(index=data.index)
    out["full_empl_prev"] = _full_empl_prev_m(
        data["full_empl_v1_prev"],
        data["full_empl_v2_prev"],
        data["survey_year"],
    )
    out["half_empl_prev"] = data["half_empl_prev"].cat.codes.between(0, 1)
    out["employed_m_prev"] = _empl_m_prev(
        out["full_empl_prev"],
        data["half_empl_prev"],
        data["mini_job_prev"],
    )
    out["months_empl_prev"] = out.groupby(["p_id", "survey_year"])[
        "employed_m_prev"
    ].transform("sum")
    return out
