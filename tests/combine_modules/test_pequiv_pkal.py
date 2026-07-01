import pandas as pd

from soep_preparation.combine_modules.pequiv_pkal import combine


def _pequiv(gesetzliche_rente_y: list[float]) -> pd.DataFrame:
    n = len(gesetzliche_rente_y)
    return pd.DataFrame(
        {
            "p_id": [1] * n,
            "survey_year": list(range(2010, 2010 + n)),
            "gesetzliche_rente_y": gesetzliche_rente_y,
            "arbeitslosengeld_y": [0.0] * n,
            "arbeitslosenhilfe_y": [0.0] * n,
        }
    )


def _pkal(number_of_months_in_retirement_last_year: list[int]) -> pd.DataFrame:
    n = len(number_of_months_in_retirement_last_year)
    return pd.DataFrame(
        {
            "p_id": [1] * n,
            "survey_year": list(range(2010, 2010 + n)),
            "number_of_months_in_retirement_last_year": (
                number_of_months_in_retirement_last_year
            ),
            "unemployment_benefits_number_of_months": [0] * n,
        }
    )


def test_combine_dates_statutory_pension_income_at_prior_year():
    """Positive statutory pension income at survey year `Y` yields first receipt `Y-1`.

    CNEF annual incomes refer to the previous calendar year, so income reported in
    wave `Y` was received in `Y - 1`.
    """
    out = combine(pequiv=_pequiv([0.0, 12000.0]), pkal=_pkal([0, 0]))

    assert out["first_pension_receipt_year"].dropna().unique().tolist() == [2010]


def test_combine_dates_prior_year_retirement_status_one_year_earlier():
    """The pkal retirement calendar reports the *previous* year.

    A person whose only retirement signal is the pkal status reported at survey
    year `Y` (`number_of_months_in_retirement_last_year > 0`) was in retirement in
    year `Y - 1`, so first receipt is dated `Y - 1`, not `Y`.
    """
    out = combine(pequiv=_pequiv([0.0, 0.0]), pkal=_pkal([0, 6]))

    assert out["first_pension_receipt_year"].dropna().unique().tolist() == [2010]
