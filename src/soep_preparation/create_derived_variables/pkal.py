"""Functions to create datasets for a pre-processed pkal dataset."""

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
    """This is on the month level."""
    out = pd.Series(pd.NA, index=full.index)
    out = out.where(~(full | half | mini_job), 1)
    return apply_lowest_int_dtype(out)


def _months_empl_prev(raw_data: pd.DataFrame, employment_sr: pd.Series) -> pd.Series:
    """This is on the annual level."""
    data = pd.concat([raw_data[["p_id", "survey_year"]], employment_sr], axis=1)
    return data.groupby(["p_id", "survey_year"])["employed_m_prev"].transform("sum")


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
    # indicating whether employed in the same month in the last year
    out["employed_m_prev"] = _empl_m_prev(
        out["full_empl_prev"].cat.codes.between(0, 23),
        data["half_empl_prev"].cat.codes.between(0, 23),
        data["mini_job_prev"].cat.codes.between(0, 11),
    )
    # indicating the number of months employed in the previous year
    out["months_empl_prev"] = _months_empl_prev(data, out["employed_m_prev"])
    return out
