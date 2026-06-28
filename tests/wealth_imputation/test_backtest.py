"""Behavior of the out-of-fold backtest scoring."""

import pandas as pd
import pytest

from soep_preparation.wealth_imputation.backtest import backtest_report


def _frame(
    predicted: list[float], *, lower: list[float], upper: list[float]
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "observed_total": [10.0, 20.0, 30.0, 40.0, 50.0],
            "point_estimate": predicted,
            "lower": lower,
            "upper": upper,
        }
    )


def test_backtest_report_perfect_prediction_has_full_quintile_accuracy():
    """When the prediction equals the truth, every quintile matches."""
    frame = _frame(
        [10.0, 20.0, 30.0, 40.0, 50.0],
        lower=[10.0, 20.0, 30.0, 40.0, 50.0],
        upper=[10.0, 20.0, 30.0, 40.0, 50.0],
    )
    report = backtest_report(frame, n_groups=5)
    assert report["exact_quintile_accuracy"] == pytest.approx(1.0)


def test_backtest_report_reversed_prediction_has_negative_rank_correlation():
    """A perfectly reversed prediction has rank correlation -1."""
    frame = _frame(
        [50.0, 40.0, 30.0, 20.0, 10.0],
        lower=[0.0] * 5,
        upper=[100.0] * 5,
    )
    report = backtest_report(frame, n_groups=5)
    assert report["rank_correlation"] == pytest.approx(-1.0)


def test_backtest_report_band_coverage_counts_observed_within_bounds():
    """Coverage is the fraction of observed values inside the imputed band."""
    frame = _frame(
        [10.0, 20.0, 30.0, 40.0, 50.0],
        lower=[0.0, 0.0, 0.0, 100.0, 100.0],  # last two bands miss the truth
        upper=[100.0, 100.0, 100.0, 200.0, 200.0],
    )
    report = backtest_report(frame, n_groups=5)
    assert report["band_coverage"] == pytest.approx(3 / 5)


def test_backtest_report_mean_abs_quintile_error_is_zero_for_perfect_prediction():
    """A perfect prediction has zero mean absolute quintile error."""
    frame = _frame(
        [10.0, 20.0, 30.0, 40.0, 50.0],
        lower=[0.0] * 5,
        upper=[100.0] * 5,
    )
    report = backtest_report(frame, n_groups=5)
    assert report["mean_abs_quintile_error"] == pytest.approx(0.0)


def _draws(values_per_household: list[float], hh_ids: list[int]) -> pd.DataFrame:
    rows = [
        {"hh_id": hh_id, "survey_year": 2017, "household_total_draw": value}
        for hh_id in hh_ids
        for value in values_per_household
    ]
    return pd.DataFrame(rows)


def test_backtest_report_draw_level_negative_share_exceeds_the_median_negative_share():
    """The draw-level imputed distribution keeps negatives the median point hides."""
    comparison = pd.DataFrame(
        {
            "hh_id": [1, 2],
            "survey_year": [2017, 2017],
            "observed_total": [80.0, 120.0],
            "point_estimate": [50.0, 150.0],
            "lower": [-100.0, -100.0],
            "upper": [200.0, 200.0],
        }
    )
    # Each household: 40% of draws negative, but the median draw is +100.
    draws = _draws([-100.0, -100.0, 100.0, 100.0, 100.0], hh_ids=[1, 2])
    report = backtest_report(comparison, imputed_draws=draws, level=0.9)
    assert report["imputed_distribution"]["negative_share"] == pytest.approx(0.0)
    assert report["imputed_distribution_across_draws"]["negative_share"][
        "mean"
    ] == pytest.approx(0.4)
