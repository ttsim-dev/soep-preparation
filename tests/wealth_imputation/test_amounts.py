"""Behavior of the PMM amount-draw orchestration."""

import numpy as np
import pytest

from soep_preparation.wealth_imputation.amounts import draw_amounts
from soep_preparation.wealth_imputation.donors import PmmResult


def test_draw_amounts_returns_observed_value_of_nearest_donor_on_asinh_scale():
    """k=1 draws the observed value of the nearest donor in asinh-scaled space."""
    rng = np.random.default_rng(seed=0)
    result = draw_amounts(
        recipient_predicted=np.array([100.0]),
        donor_predicted=np.array([10.0, 100.0, 1000.0]),
        donor_observed=np.array([12.0, 90.0, 1100.0]),
        scale=100.0,
        k=1,
        rng=rng,
    )
    # asinh(100/100) matches the second donor exactly; its observed value is 90.
    np.testing.assert_allclose(result.values, [90.0], atol=1e-6)


def test_draw_amounts_caliper_applies_on_the_asinh_scaled_axis():
    """A recipient far from every donor on the scaled axis fails the caliper."""
    rng = np.random.default_rng(seed=0)
    with pytest.raises(ValueError, match="No eligible donor"):
        draw_amounts(
            recipient_predicted=np.array([1e9]),
            donor_predicted=np.array([10.0, 100.0]),
            donor_observed=np.array([12.0, 90.0]),
            scale=100.0,
            k=1,
            rng=rng,
            caliper=1.0,
        )


def _draw_once(seed: int) -> PmmResult:
    return draw_amounts(
        recipient_predicted=np.array([50.0]),
        donor_predicted=np.array([10.0, 100.0, 1000.0]),
        donor_observed=np.array([12.0, 90.0, 1100.0]),
        scale=100.0,
        k=2,
        rng=np.random.default_rng(seed=seed),
    )


def test_draw_amounts_is_deterministic_for_a_fixed_seed():
    """Same seed and inputs reproduce the drawn values and donor indices."""
    first = _draw_once(seed=7)
    second = _draw_once(seed=7)
    np.testing.assert_array_equal(first.values, second.values)
    np.testing.assert_array_equal(first.donor_indices, second.donor_indices)


@pytest.mark.parametrize("scale", [0.0, -1.0, np.inf])
def test_draw_amounts_rejects_invalid_scale(scale: float):
    """A non-positive or non-finite component scale fails closed."""
    with pytest.raises(ValueError, match="scale"):
        draw_amounts(
            recipient_predicted=np.array([100.0]),
            donor_predicted=np.array([100.0]),
            donor_observed=np.array([90.0]),
            scale=scale,
            k=1,
            rng=np.random.default_rng(seed=0),
        )
