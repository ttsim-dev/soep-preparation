"""First statutory-pension receipt year from pequiv + pkal.

Despite the generic `pequiv_pkal` module name (combine modules are named by the
two modules they join), this produces exactly one derived variable:
`first_pension_receipt_year`. It is not a general pequiv x pkal merge.
"""

import pandas as pd

from soep_preparation.utilities.data_manipulator import apply_smallest_int_dtype


def combine(pequiv: pd.DataFrame, pkal: pd.DataFrame) -> pd.DataFrame:
    """Derive `first_pension_receipt_year` only.

    The earliest year in which a person either reports positive annual statutory
    pension income (`gesetzliche_rente_y`, pequiv, dated at its survey year) or any
    retirement month in the calendar of the previous year
    (`number_of_months_in_retirement_last_year`, pkal, dated at `survey_year - 1`
    because that calendar item refers to the year before the survey). The consuming
    project turns it into a claiming age with the birth year.

    The pkal retirement-calendar branch is a labour-market-*status* proxy (raw label
    "Rente, Pension, Vorruhestand"), not validated statutory-benefit receipt: it can
    flag early exit / Vorruhestand that is not statutory pension claiming. It is kept
    here as a fallback signal with its correct reference year, but whether such a
    status should feed "first statutory pension receipt" at all is flagged for
    maintainer review (see clean_modules/pkal.py docstring on the same item).

    Args:
        pequiv: Cleaned pequiv module (statutory pension income).
        pkal: Cleaned pkal module (previous-year retirement calendar).

    Returns:
        Person-year frame carrying the (time-invariant) first pension-receipt year.
    """
    merged = pd.merge(
        pequiv[["p_id", "survey_year", "gesetzliche_rente_y"]],
        pkal[["p_id", "survey_year", "number_of_months_in_retirement_last_year"]],
        on=["p_id", "survey_year"],
        how="outer",
    )

    # Statutory pension income refers to the survey year itself.
    has_pension_income = merged["gesetzliche_rente_y"].fillna(value=0) > 0
    income_receipt_year = (
        merged.loc[has_pension_income].groupby("p_id")["survey_year"].min()
    )

    # The pkal retirement calendar refers to the *previous* year, so its candidate
    # receipt year is `survey_year - 1`. This OR-branch is a labour-market-status
    # proxy, not validated statutory receipt (flagged for review; see docstring).
    has_prior_year_retirement = (
        merged["number_of_months_in_retirement_last_year"].fillna(value=0) > 0
    )
    status_receipt_year = (
        merged.loc[has_prior_year_retirement, "survey_year"]
        .sub(1)
        .groupby(merged["p_id"])
        .min()
    )

    first_receipt_year = (
        pd.concat([income_receipt_year, status_receipt_year]).groupby(level=0).min()
    )

    out = pd.DataFrame(index=merged.index)
    out["p_id"] = merged["p_id"]
    out["survey_year"] = merged["survey_year"]
    out["first_pension_receipt_year"] = apply_smallest_int_dtype(
        merged["p_id"].map(first_receipt_year)
    )
    return out
