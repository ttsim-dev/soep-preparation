"""Tests for the pequiv cleaning module."""

import pandas as pd

from soep_preparation.clean_modules.pequiv import (
    _calculate_dependent_employment_income,
)


def test_dependent_employment_income_nets_out_self_employment() -> None:
    """Dependent-employment income is labour earnings minus self-employment income."""
    expected = pd.Series([30000.0, 0.0, 12000.0], dtype="float64[pyarrow]")

    actual = _calculate_dependent_employment_income(
        labour_earnings=pd.Series([30000.0, 40000.0, 50000.0], dtype="float"),
        self_employment_income=pd.Series([0.0, 40000.0, 38000.0], dtype="float"),
    )

    pd.testing.assert_series_equal(actual, expected)


def test_dependent_employment_income_is_zero_when_self_employment_larger() -> None:
    """Rounding in the reported aggregates never yields negative earnings."""
    expected = pd.Series([0.0], dtype="float64[pyarrow]")

    actual = _calculate_dependent_employment_income(
        labour_earnings=pd.Series([20000.0], dtype="float"),
        self_employment_income=pd.Series([20001.0], dtype="float"),
    )

    pd.testing.assert_series_equal(actual, expected)


def test_dependent_employment_income_is_missing_if_an_input_is_missing() -> None:
    """A missing input aggregate leaves the derived variable missing."""
    expected = pd.Series([pd.NA, pd.NA], dtype="float64[pyarrow]")

    actual = _calculate_dependent_employment_income(
        labour_earnings=pd.Series([pd.NA, 30000.0], dtype="float64[pyarrow]"),
        self_employment_income=pd.Series([10000.0, pd.NA], dtype="float64[pyarrow]"),
    )

    pd.testing.assert_series_equal(actual, expected)
