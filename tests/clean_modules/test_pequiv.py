"""Tests for the pequiv cleaning module."""

import pandas as pd

from soep_preparation.clean_modules.pequiv import (
    _calculate_dependent_employment_income,
)


def _make_series(*values: float | None) -> pd.Series:
    return pd.Series(values, dtype="float64[pyarrow]")


def test_dependent_employment_income_sums_wage_and_bonus_components() -> None:
    """Dependent-employment income adds up the non-self-employed pay components."""
    expected = _make_series(36500.0)

    actual = _calculate_dependent_employment_income(
        earnings_from_first_job_y=_make_series(30000.0),
        earnings_from_second_job_y=_make_series(2000.0),
        thirteenth_monthly_salary_y=_make_series(2500.0),
        fourteenth_monthly_salary_y=_make_series(0.0),
        christmas_bonus_y=_make_series(1000.0),
        holiday_bonus_y=_make_series(500.0),
        profit_sharing_y=_make_series(300.0),
        other_bonuses_y=_make_series(200.0),
    )

    pd.testing.assert_series_equal(actual, expected)


def test_dependent_employment_income_ignores_missing_components() -> None:
    """A missing component does not wipe out the reported ones."""
    expected = _make_series(31000.0)

    actual = _calculate_dependent_employment_income(
        earnings_from_first_job_y=_make_series(30000.0),
        earnings_from_second_job_y=_make_series(None),
        thirteenth_monthly_salary_y=_make_series(None),
        fourteenth_monthly_salary_y=_make_series(None),
        christmas_bonus_y=_make_series(1000.0),
        holiday_bonus_y=_make_series(None),
        profit_sharing_y=_make_series(None),
        other_bonuses_y=_make_series(None),
    )

    pd.testing.assert_series_equal(actual, expected)
