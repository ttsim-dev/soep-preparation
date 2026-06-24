"""Behavior of household-total interval assembly."""

import numpy as np
import pandas as pd
import pytest

from soep_preparation.wealth_imputation.intervals import (
    distribution_across_draws,
    household_total_intervals,
)


def _draws(values: list[float], hh_id: int = 1) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "hh_id": [hh_id] * len(values),
            "survey_year": [2017] * len(values),
            "household_total_draw": values,
        }
    )


def test_household_total_intervals_returns_median_and_central_bounds():
    """Point estimate is the median; bounds are the central-`level` quantiles."""
    result = household_total_intervals(
        _draws([100.0, 200.0, 300.0, 400.0, 500.0]), level=0.8
    )
    row = result.iloc[0]
    np.testing.assert_allclose(row["point_estimate"], 300.0, atol=1e-6)
    np.testing.assert_allclose(row["lower"], 140.0, atol=1e-6)
    np.testing.assert_allclose(row["upper"], 460.0, atol=1e-6)


def test_household_total_intervals_summarises_each_household_separately():
    """Each household's interval uses only its own draws."""
    draws = pd.concat(
        [
            _draws([10.0, 20.0, 30.0], hh_id=1),
            _draws([100.0, 200.0, 300.0], hh_id=2),
        ],
        ignore_index=True,
    )
    result = household_total_intervals(draws, level=0.5).set_index("hh_id")
    np.testing.assert_allclose(result.loc[1, "point_estimate"], 20.0, atol=1e-6)
    np.testing.assert_allclose(result.loc[2, "point_estimate"], 200.0, atol=1e-6)


@pytest.mark.parametrize("level", [0.0, 1.0, -0.1, 1.5])
def test_household_total_intervals_rejects_level_outside_unit_interval(level: float):
    """A coverage level outside (0, 1) fails closed."""
    with pytest.raises(ValueError, match="level"):
        household_total_intervals(_draws([1.0, 2.0, 3.0]), level=level)


def test_zero_mass_survives_in_within_draw_distribution_but_not_in_medians():
    """Per-household medians erase a zero mass that each complete draw preserves.

    With 100 households each drawn 200 times -- 80 zeros, 120 at 100 -- every complete
    draw has a 40% zero share, so the predictive distribution does too. The
    cross-section of household medians has no zeros at all, which is why distribution
    statistics must be computed within each draw and then summarised across draws.
    """
    rows = [(h, 2022, v) for h in range(100) for v in ([0.0] * 80 + [100.0] * 120)]
    draws = pd.DataFrame(rows, columns=["hh_id", "survey_year", "household_total_draw"])
    points = household_total_intervals(draws, level=0.9)
    within = distribution_across_draws(draws)
    np.testing.assert_allclose(points["point_estimate"].eq(0.0).mean(), 0.0)
    np.testing.assert_allclose(within["zero_share"]["mean"], 0.4, atol=1e-9)


def test_household_total_intervals_returns_float64_bounds():
    """Bounds are float64 even when draws are integer-typed."""
    draws = pd.DataFrame(
        {
            "hh_id": [1, 1, 1],
            "survey_year": [2017, 2017, 2017],
            "household_total_draw": pd.array([10, 20, 30], dtype="int64"),
        }
    )
    result = household_total_intervals(draws)
    assert result["lower"].dtype == np.float64
    assert result["upper"].dtype == np.float64
