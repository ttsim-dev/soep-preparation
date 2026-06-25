"""Tests for the combined last-year unemployment-benefit receipt indicator.

The indicator `received_unemployment_benefits_last_year` lives in
`combine_modules/pequiv_pkal.combine` and unifies three signals that all refer
to the previous calendar year:

- `pkal.unemployment_benefits_number_of_months` (raw `kal2f02`): months of
  Arbeitslosengeld receipt.
- `pequiv.arbeitslosengeld_y` (raw `iunby`): annual euro amount of
  Arbeitslosengeld.
- `pequiv.arbeitslosenhilfe_y` (raw `iunay`, 1984 through 2005): annual euro
  amount of the means-tested Arbeitslosenhilfe.
"""

import pandas as pd

from soep_preparation.combine_modules.pequiv_pkal import combine


def _pequiv(
    arbeitslosengeld_y: float,
    arbeitslosenhilfe_y: float,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "p_id": [1],
            "survey_year": [2003],
            "gesetzliche_rente_y": [0.0],
            "arbeitslosengeld_y": [arbeitslosengeld_y],
            "arbeitslosenhilfe_y": [arbeitslosenhilfe_y],
        }
    )


def _pkal(unemployment_benefits_number_of_months: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "p_id": [1],
            "survey_year": [2003],
            "number_of_months_in_retirement_last_year": [0],
            "unemployment_benefits_number_of_months": [
                unemployment_benefits_number_of_months
            ],
        }
    )


def test_received_unemployment_benefits_true_when_pkal_months_positive():
    """A positive pkal month count marks last-year unemployment-benefit receipt."""
    out = combine(
        pequiv=_pequiv(arbeitslosengeld_y=0.0, arbeitslosenhilfe_y=0.0),
        pkal=_pkal(unemployment_benefits_number_of_months=4),
    )
    assert out["received_unemployment_benefits_last_year"].tolist() == [True]


def test_received_unemployment_benefits_filled_from_arbeitslosengeld_amount():
    """A positive Arbeitslosengeld amount fills a zero pkal month count.

    `arbeitslosengeld_y` is the annual euro amount of the insurance benefit and
    refers to the previous calendar year, the same reference period as the pkal
    month count, so a positive amount implies receipt even with no pkal months.
    """
    out = combine(
        pequiv=_pequiv(arbeitslosengeld_y=3600.0, arbeitslosenhilfe_y=0.0),
        pkal=_pkal(unemployment_benefits_number_of_months=0),
    )
    assert out["received_unemployment_benefits_last_year"].tolist() == [True]


def test_received_unemployment_benefits_filled_from_arbeitslosenhilfe_amount():
    """A positive Arbeitslosenhilfe amount also marks benefit receipt.

    Arbeitslosenhilfe is a distinct, means-tested programme, but for a combined
    "received any unemployment benefit last year" signal a positive amount counts.
    """
    out = combine(
        pequiv=_pequiv(arbeitslosengeld_y=0.0, arbeitslosenhilfe_y=2400.0),
        pkal=_pkal(unemployment_benefits_number_of_months=0),
    )
    assert out["received_unemployment_benefits_last_year"].tolist() == [True]


def test_received_unemployment_benefits_false_when_no_signal():
    """No months and no positive amount means no benefit receipt last year."""
    out = combine(
        pequiv=_pequiv(arbeitslosengeld_y=0.0, arbeitslosenhilfe_y=0.0),
        pkal=_pkal(unemployment_benefits_number_of_months=0),
    )
    assert out["received_unemployment_benefits_last_year"].tolist() == [False]


def test_received_unemployment_benefits_dtype_is_pyarrow_boolean():
    """The combined receipt indicator is a pyarrow-backed boolean."""
    out = combine(
        pequiv=_pequiv(arbeitslosengeld_y=3600.0, arbeitslosenhilfe_y=0.0),
        pkal=_pkal(unemployment_benefits_number_of_months=0),
    )
    assert out["received_unemployment_benefits_last_year"].dtype == "bool[pyarrow]"


def test_received_unemployment_benefits_na_amount_does_not_imply_receipt():
    """A missing pequiv amount with no pkal months is treated as no receipt."""
    pequiv = pd.DataFrame(
        {
            "p_id": [1],
            "survey_year": [2003],
            "gesetzliche_rente_y": [0.0],
            "arbeitslosengeld_y": [pd.NA],
            "arbeitslosenhilfe_y": [pd.NA],
        }
    )
    out = combine(
        pequiv=pequiv,
        pkal=_pkal(unemployment_benefits_number_of_months=0),
    )
    assert out["received_unemployment_benefits_last_year"].tolist() == [False]
