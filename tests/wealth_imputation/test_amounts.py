"""Behavior of the PMM amount-draw orchestration.

`draw_amounts` matches on the asinh axis passed in (the amount model's `predict_score`),
so callers transform once at the model and the draw never re-applies a transform.
"""

import numpy as np
import pytest

from soep_preparation.wealth_imputation.amounts import draw_amounts
from soep_preparation.wealth_imputation.donors import PmmResult

_SCALE = 100.0


def _asinh(values: np.ndarray) -> np.ndarray:
    return np.arcsinh(values / _SCALE)


def test_draw_amounts_returns_observed_value_of_nearest_donor():
    """k=1 draws the observed value of the nearest donor on the supplied score axis."""
    rng = np.random.default_rng(seed=0)
    result = draw_amounts(
        recipient_predicted=_asinh(np.array([100.0])),
        donor_predicted=_asinh(np.array([10.0, 100.0, 1000.0])),
        donor_observed=np.array([12.0, 90.0, 1100.0]),
        scale=_SCALE,
        k=1,
        rng=rng,
    )
    # The recipient score matches the second donor exactly; its observed value is 90.
    np.testing.assert_allclose(result.values, [90.0], atol=1e-6)


def test_draw_amounts_caliper_applies_on_the_supplied_score_axis():
    """A recipient far from every donor on the score axis fails the caliper."""
    rng = np.random.default_rng(seed=0)
    with pytest.raises(ValueError, match="No eligible donor"):
        draw_amounts(
            recipient_predicted=_asinh(np.array([1e9])),
            donor_predicted=_asinh(np.array([10.0, 100.0])),
            donor_observed=np.array([12.0, 90.0]),
            scale=_SCALE,
            k=1,
            rng=rng,
            caliper=1.0,
        )


def _draw_once(seed: int) -> PmmResult:
    return draw_amounts(
        recipient_predicted=_asinh(np.array([50.0])),
        donor_predicted=_asinh(np.array([10.0, 100.0, 1000.0])),
        donor_observed=np.array([12.0, 90.0, 1100.0]),
        scale=_SCALE,
        k=2,
        rng=np.random.default_rng(seed=seed),
    )


def test_draw_amounts_is_deterministic_for_a_fixed_seed():
    """Same seed and inputs reproduce the drawn values and donor indices."""
    first = _draw_once(seed=7)
    second = _draw_once(seed=7)
    np.testing.assert_array_equal(first.values, second.values)
    np.testing.assert_array_equal(first.donor_indices, second.donor_indices)


def test_draw_amounts_does_not_back_transform_then_re_transform_the_scores():
    """Extreme scores match without the `sinh`-then-`asinh` overflow of the round trip.

    Passing a far-out-of-support recipient score must still pick the nearest donor by
    score distance; the previous round trip would `sinh` the score to `inf` and corrupt
    the match.
    """
    rng = np.random.default_rng(seed=0)
    result = draw_amounts(
        recipient_predicted=np.array([40.0]),  # huge asinh score, finite
        donor_predicted=np.array([1.0, 39.0, 40.0]),
        donor_observed=np.array([1.0, 2.0, 3.0]),
        scale=_SCALE,
        k=1,
        rng=rng,
    )
    np.testing.assert_allclose(result.values, [3.0], atol=1e-9)
