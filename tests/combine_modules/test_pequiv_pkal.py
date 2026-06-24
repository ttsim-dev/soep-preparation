import pandas as pd

from soep_preparation.combine_modules.pequiv_pkal import combine


def test_combine_dates_statutory_pension_income_at_survey_year():
    """Positive statutory pension income at survey year `Y` yields first receipt `Y`."""
    pequiv = pd.DataFrame(
        {
            "p_id": [1, 1],
            "survey_year": [2010, 2011],
            "gesetzliche_rente_y": [0.0, 12000.0],
        }
    )
    pkal = pd.DataFrame(
        {
            "p_id": [1, 1],
            "survey_year": [2010, 2011],
            "number_of_months_in_retirement_last_year": [0, 0],
        }
    )

    out = combine(pequiv=pequiv, pkal=pkal)

    assert out["first_pension_receipt_year"].dropna().unique().tolist() == [2011]


def test_combine_dates_prior_year_retirement_status_one_year_earlier():
    """The pkal retirement calendar reports the *previous* year.

    A person whose only retirement signal is the pkal status reported at survey
    year `Y` (`number_of_months_in_retirement_last_year > 0`) was in retirement in
    year `Y - 1`, so first receipt is dated `Y - 1`, not `Y`.
    """
    pequiv = pd.DataFrame(
        {
            "p_id": [2, 2],
            "survey_year": [2010, 2011],
            "gesetzliche_rente_y": [0.0, 0.0],
        }
    )
    pkal = pd.DataFrame(
        {
            "p_id": [2, 2],
            "survey_year": [2010, 2011],
            "number_of_months_in_retirement_last_year": [0, 6],
        }
    )

    out = combine(pequiv=pequiv, pkal=pkal)

    assert out["first_pension_receipt_year"].dropna().unique().tolist() == [2010]
