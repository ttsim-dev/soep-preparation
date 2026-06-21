"""First own-pension receipt year (claiming-age input)."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import apply_smallest_int_dtype


def combine(pequiv: pd.DataFrame, pkal: pd.DataFrame) -> pd.DataFrame:
    """First survey year a person draws a statutory pension.

    Combines annual statutory pension income (`gesetzliche_rente_y`, pequiv) with
    the monthly pension calendar count (`number_of_months_in_pension`, pkal). The
    earliest year either is positive is the claiming year; rmsm-data turns it into
    a claiming age with the birth year.

    Args:
        pequiv: Cleaned pequiv module (statutory pension income).
        pkal: Cleaned pkal module (monthly pension calendar).

    Returns:
        Person-year frame carrying the (time-invariant) first pension-receipt year.
    """
    merged = pd.merge(
        pequiv[["p_id", "survey_year", "gesetzliche_rente_y"]],
        pkal[["p_id", "survey_year", "number_of_months_in_pension"]],
        on=["p_id", "survey_year"],
        how="outer",
    )
    receives_pension = (merged["gesetzliche_rente_y"].fillna(value=0) > 0) | (
        merged["number_of_months_in_pension"].fillna(value=0) > 0
    )
    first_receipt_year = (
        merged.loc[receives_pension].groupby("p_id")["survey_year"].min()
    )

    out = pd.DataFrame(index=merged.index)
    out["p_id"] = merged["p_id"]
    out["survey_year"] = merged["survey_year"]
    out["first_pension_receipt_year"] = apply_smallest_int_dtype(
        merged["p_id"].map(first_receipt_year)
    )
    return out
