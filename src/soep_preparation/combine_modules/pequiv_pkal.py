"""Derived variables that join pequiv and pkal.

Despite the generic `pequiv_pkal` module name (combine modules are named by the
two modules they join), this produces two specific derived variables rather than a
general pequiv x pkal merge:

- `first_pension_receipt_year`: first observed year of pension income or a
  retirement-status proxy, not validated statutory pension receipt.
- `received_unemployment_benefits_last_year`: whether the person drew any
  unemployment benefit in the previous calendar year.
"""

import pandas as pd

from soep_preparation.utilities.data_manipulator import apply_smallest_int_dtype


def combine(pequiv: pd.DataFrame, pkal: pd.DataFrame) -> pd.DataFrame:
    """Derive pension-receipt timing and last-year unemployment-benefit receipt.

    `first_pension_receipt_year` is a *first observed pension-income or
    retirement-status proxy year*, not validated statutory pension receipt. It is the
    earliest year in which a person either reports positive annual pension income
    (`gesetzliche_rente_y`, pequiv, dated at `survey_year - 1` because CNEF annual
    incomes refer to the previous calendar year) or any retirement month in the
    calendar of the previous year (`number_of_months_in_retirement_last_year`, pkal,
    dated at `survey_year - 1` because that calendar item refers to the year before the
    survey). The consuming project turns it into a claiming age with the birth year.
    The pkal branch is a labour-market-*status* proxy (raw label "Rente, Pension,
    Vorruhestand"): it can flag early exit / Vorruhestand that is not statutory pension
    claiming. It is kept as a fallback signal with its correct reference year, but
    whether such a status should feed a statutory-receipt concept at all is flagged for
    maintainer review (see clean_modules/pkal.py).

    `received_unemployment_benefits_last_year` unifies three previous-calendar-year
    signals (see `_received_unemployment_benefits_last_year`).

    Args:
        pequiv: Cleaned pequiv module (pension income, unemployment-benefit amounts).
        pkal: Cleaned pkal module (previous-year retirement and unemployment calendar).

    Returns:
        Person-year frame with `first_pension_receipt_year` and
        `received_unemployment_benefits_last_year`.
    """
    merged = pd.merge(
        pequiv[
            [
                "p_id",
                "survey_year",
                "gesetzliche_rente_y",
                "arbeitslosengeld_y",
                "arbeitslosenhilfe_y",
            ]
        ],
        pkal[
            [
                "p_id",
                "survey_year",
                "number_of_months_in_retirement_last_year",
                "unemployment_benefits_number_of_months",
            ]
        ],
        on=["p_id", "survey_year"],
        how="outer",
    )

    # CNEF annual pension income (`igrv1`) refers to the previous calendar year, so a
    # positive value in wave `t` means receipt in `t - 1`.
    has_pension_income = merged["gesetzliche_rente_y"].fillna(value=0) > 0
    income_receipt_year = (
        merged.loc[has_pension_income, "survey_year"]
        .sub(1)
        .groupby(merged["p_id"])
        .min()
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
    out["received_unemployment_benefits_last_year"] = (
        _received_unemployment_benefits_last_year(
            unemployment_benefits_number_of_months=merged[
                "unemployment_benefits_number_of_months"
            ],
            arbeitslosengeld_y=merged["arbeitslosengeld_y"],
            arbeitslosenhilfe_y=merged["arbeitslosenhilfe_y"],
        )
    )
    return out


def _received_unemployment_benefits_last_year(
    unemployment_benefits_number_of_months: pd.Series,
    arbeitslosengeld_y: pd.Series,
    arbeitslosenhilfe_y: pd.Series,
) -> pd.Series:
    """Flag any unemployment-benefit receipt in the previous calendar year.

    The issue motivating this variable claims pequiv `iunay` imputes missing pkal
    `kal2f02`. That premise does not hold literally: `kal2f02`
    (`unemployment_benefits_number_of_months`) counts months of Arbeitslosengeld,
    whereas `iunay` (`arbeitslosenhilfe_y`) is an annual euro amount of the distinct,
    means-tested Arbeitslosenhilfe (available 1984 through 2005). The unit (months
    vs euros) and the programme (Arbeitslosengeld vs Arbeitslosenhilfe) both differ,
    so one cannot directly impute the other.

    What is defensible is a single receipt indicator over the previous calendar year,
    the common reference period of all three inputs:

    - `unemployment_benefits_number_of_months` (pkal `kal2f02`): months of
      Arbeitslosengeld; positive ⇒ receipt.
    - `arbeitslosengeld_y` (pequiv `iunby`): annual Arbeitslosengeld amount; the
      amount-side counterpart of the pkal month count; positive ⇒ receipt.
    - `arbeitslosenhilfe_y` (pequiv `iunay`): annual Arbeitslosenhilfe amount;
      positive ⇒ receipt of that distinct benefit.

    The indicator is tri-state:

    - `True` if any of the three signals is positive;
    - `False` if at least one signal is observed and none is positive;
    - `pd.NA` if all three signals are missing, so "no source observed" is not
      silently turned into a definitive "did not receive".

    Args:
        unemployment_benefits_number_of_months: Months of Arbeitslosengeld (pkal).
        arbeitslosengeld_y: Annual Arbeitslosengeld amount in euros (pequiv).
        arbeitslosenhilfe_y: Annual Arbeitslosenhilfe amount in euros (pequiv).

    Returns:
        Nullable boolean Series: `True`/`False` where any signal is observed, `pd.NA`
        where every signal is missing.
    """
    positive = (
        (unemployment_benefits_number_of_months.fillna(value=0) > 0)
        | (arbeitslosengeld_y.fillna(value=0) > 0)
        | (arbeitslosenhilfe_y.fillna(value=0) > 0)
    )
    any_observed = (
        unemployment_benefits_number_of_months.notna()
        | arbeitslosengeld_y.notna()
        | arbeitslosenhilfe_y.notna()
    )
    received = positive.astype("bool[pyarrow]").where(any_observed, other=pd.NA)
    return received.astype("bool[pyarrow]")
