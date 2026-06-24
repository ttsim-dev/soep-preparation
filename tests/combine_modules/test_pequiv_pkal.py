"""Test the pequiv_pkal combine module."""

import pandas as pd

from soep_preparation.combine_modules.pequiv_pkal import combine


def test_first_pension_receipt_year_uses_reference_year_from_pension_income():
    """The receipt year is the calendar year the pension income refers to (t-1).

    `gesetzliche_rente_y` is the previous calendar year's statutory pension
    income, so a positive value reported in survey year `t` means the person
    received a pension in `t-1`. The receipt year must therefore be `t-1`.
    """
    pequiv = pd.DataFrame(
        {"p_id": [1], "survey_year": [2022], "gesetzliche_rente_y": [100.0]}
    )
    pkal = pd.DataFrame(
        {
            "p_id": [1],
            "survey_year": [2022],
            "number_of_months_in_retirement_last_year": [0],
        }
    )

    result = combine(pequiv=pequiv, pkal=pkal)

    assert result["first_pension_receipt_year"].item() == 2021


def test_first_pension_receipt_year_uses_reference_year_from_retirement_months():
    """The receipt year is `t-1` when only the retirement-month count is positive.

    `number_of_months_in_retirement_last_year` counts retirement months in the
    previous calendar year, so a positive count in survey year `t` means the
    person was retired in `t-1`.
    """
    pequiv = pd.DataFrame(
        {"p_id": [1], "survey_year": [2022], "gesetzliche_rente_y": [0.0]}
    )
    pkal = pd.DataFrame(
        {
            "p_id": [1],
            "survey_year": [2022],
            "number_of_months_in_retirement_last_year": [3],
        }
    )

    result = combine(pequiv=pequiv, pkal=pkal)

    assert result["first_pension_receipt_year"].item() == 2021


def test_first_pension_receipt_year_is_earliest_reference_year():
    """The receipt year is the earliest `t-1` across all waves with receipt."""
    pequiv = pd.DataFrame(
        {
            "p_id": [1, 1, 1],
            "survey_year": [2020, 2021, 2022],
            "gesetzliche_rente_y": [0.0, 100.0, 200.0],
        }
    )
    pkal = pd.DataFrame(
        {
            "p_id": [1, 1, 1],
            "survey_year": [2020, 2021, 2022],
            "number_of_months_in_retirement_last_year": [0, 0, 0],
        }
    )

    result = combine(pequiv=pequiv, pkal=pkal)

    assert (result["first_pension_receipt_year"] == 2020).all()
