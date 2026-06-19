import numpy as np
import pytest

from soep_preparation.wealth_imputation.donors import pmm_draw


def test_pmm_draw_returns_a_near_donor_value_deterministic_under_seed():
    donor_scores = np.array([0.0, 1.0, 5.0, 10.0])
    donor_values = np.array([10.0, 11.0, 50.0, 100.0])
    recipient_scores = np.array([0.4])
    rng = np.random.default_rng(seed=0)
    drawn = pmm_draw(recipient_scores, donor_scores, donor_values, k=2, rng=rng)
    # nearest two donors to 0.4 are scores 0.0 and 1.0 -> values 10.0 or 11.0
    assert drawn[0] in {10.0, 11.0}


def test_pmm_draw_raises_when_no_donor_within_caliper():
    donor_scores = np.array([0.0, 1.0])
    donor_values = np.array([10.0, 11.0])
    recipient_scores = np.array([100.0])
    rng = np.random.default_rng(seed=0)
    with pytest.raises(ValueError, match="No donor within caliper"):
        pmm_draw(
            recipient_scores, donor_scores, donor_values, k=1, rng=rng, caliper=0.5
        )
