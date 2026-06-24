"""First statutory-pension receipt year from pequiv + pkal.

Despite the generic `pequiv_pkal` module name (combine modules are named by the
two modules they join), this produces exactly one derived variable:
`first_pension_receipt_year`. It is not a general pequiv x pkal merge.
"""

import pandas as pd

from soep_preparation.utilities.data_manipulator import apply_smallest_int_dtype


def combine(pequiv: pd.DataFrame, pkal: pd.DataFrame) -> pd.DataFrame:
    """Derive `first_pension_receipt_year` only.

    The earliest calendar year in which a person either reports positive annual
    statutory pension income (`gesetzliche_rente_y`, pequiv) or any retirement
    month (`number_of_months_in_retirement_last_year`, pkal). Both inputs are
    retrospective: a value reported in survey year `t` describes the previous
    calendar year `t - 1`. The receipt year is therefore `survey_year - 1`. The
    consuming project turns it into a claiming age with the birth year.

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
    receives_pension = (merged["gesetzliche_rente_y"].fillna(value=0) > 0) | (
        merged["number_of_months_in_retirement_last_year"].fillna(value=0) > 0
    )
    reference_year = merged["survey_year"] - 1
    first_receipt_year = (
        reference_year.loc[receives_pension].groupby(merged["p_id"]).min()
    )

    out = pd.DataFrame(index=merged.index)
    out["p_id"] = merged["p_id"]
    out["survey_year"] = merged["survey_year"]
    out["first_pension_receipt_year"] = apply_smallest_int_dtype(
        merged["p_id"].map(first_receipt_year)
    )
    return out
