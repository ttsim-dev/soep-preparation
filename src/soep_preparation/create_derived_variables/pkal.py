"""Functions to create pre-processed pkal data."""

import pandas as pd
from pandas.api.types import union_categoricals

from soep_preparation.utilities.series_manipulator import apply_lowest_int_dtype


def _ft_employed_in_month(
    v1_m: "pd.Series[pd.Categorical]",
    v2_m: "pd.Series[pd.Categorical]",
    survey_year: "pd.Series[int]",
) -> "pd.Series[pd.Categorical]":
    v1_m_aligned = v1_m.cat.set_categories(v2_m.cat.categories)
    return pd.Series(
        union_categoricals(
            [
                v1_m_aligned.where(survey_year < 1997, v2_m),  # noqa: PLR2004
                v2_m,
            ],
        ),
    )


def _employed_in_month(
    full: "pd.Series[pd.Categorical]",
    half: "pd.Series[pd.Categorical]",
    minijob: "pd.Series[pd.Categorical]",
) -> "pd.Series[int]":
    """This is on the month level."""
    out = pd.Series(pd.NA, index=full.index)
    out = out.where(~(full | half | minijob), 1)
    return apply_lowest_int_dtype(out)


def _number_of_months_employed(
    raw_data: pd.DataFrame, employment_sr: pd.Series
) -> pd.Series:
    """This is on the annual level."""
    data = pd.concat([raw_data[["p_id", "survey_year"]], employment_sr], axis=1)
    return data.groupby(["p_id", "survey_year"])["employed_in_month"].transform("sum")


def create_derived_variables(data: pd.DataFrame) -> pd.DataFrame:
    """Create derived variables for the pkal data.

    Args:
        data: The required data.

    Returns:
        Derived variables.
    """
    out = pd.DataFrame(index=data.index)

    # aligning the two consecutive full-time employment variables
    out["ft_employed"] = _ft_employed_in_month(
        data["ft_employed_v1"],
        data["ft_employed_v2"],
        data["survey_year"],
    )
    # indicating whether employed at all in a month in the last year
    out["employed_in_month"] = _employed_in_month(
        out["ft_employed"].cat.codes.between(0, 23),
        data["pt_employed"].cat.codes.between(0, 23),
        data["minijob_employed"].cat.codes.between(0, 11),
    )
    # indicating the number of months employed in the previous year
    out["number_of_months_employed"] = _number_of_months_employed(
        data, out["employed_in_month"]
    )
    return out
