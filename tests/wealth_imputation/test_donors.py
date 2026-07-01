import math

import numpy as np
import pytest

from soep_preparation.wealth_imputation.donors import pmm_draw


def test_pmm_draw_is_deterministic_under_same_seed():
    donor_scores = np.array([0.0, 1.0, 5.0, 10.0])
    donor_values = np.array([10.0, 11.0, 50.0, 100.0])
    recipient_scores = np.array([0.4, 6.0])
    first = pmm_draw(
        recipient_scores,
        donor_scores,
        donor_values,
        k=2,
        rng=np.random.default_rng(seed=0),
    )
    second = pmm_draw(
        recipient_scores,
        donor_scores,
        donor_values,
        k=2,
        rng=np.random.default_rng(seed=0),
    )
    np.testing.assert_array_equal(first.values, second.values)
    np.testing.assert_array_equal(first.donor_indices, second.donor_indices)


def test_pmm_draw_never_selects_donor_outside_caliper():
    # Nearest donor (0.1) is inside the caliper; the other (100.0) is outside it.
    donor_scores = np.array([0.1, 100.0])
    donor_values = np.array([10.0, 999.0])
    recipient_scores = np.array([0.0])
    rng = np.random.default_rng(seed=1)
    for _ in range(20):
        result = pmm_draw(
            recipient_scores, donor_scores, donor_values, k=2, rng=rng, caliper=0.2
        )
        assert result.donor_indices[0] == 0
        assert math.isclose(result.values[0], 10.0)


def test_pmm_draw_never_selects_excluded_donor():
    donor_scores = np.array([0.0, 1.0])
    donor_values = np.array([10.0, 11.0])
    recipient_scores = np.array([0.0])
    rng = np.random.default_rng(seed=0)
    for _ in range(20):
        result = pmm_draw(
            recipient_scores, donor_scores, donor_values, k=1, rng=rng, exclude=[[0]]
        )
        assert result.donor_indices[0] == 1
        assert math.isclose(result.values[0], 11.0)


def test_pmm_draw_raises_when_no_donor_within_caliper():
    donor_scores = np.array([0.0, 1.0])
    donor_values = np.array([10.0, 11.0])
    recipient_scores = np.array([100.0])
    rng = np.random.default_rng(seed=0)
    with pytest.raises(ValueError, match="No eligible donor"):
        pmm_draw(
            recipient_scores, donor_scores, donor_values, k=1, rng=rng, caliper=0.5
        )


def test_pmm_draw_rejects_k_below_one():
    rng = np.random.default_rng(seed=0)
    with pytest.raises(ValueError, match="k must be >= 1"):
        pmm_draw(np.array([0.0]), np.array([0.0]), np.array([1.0]), k=0, rng=rng)


def test_pmm_draw_rejects_empty_donors():
    rng = np.random.default_rng(seed=0)
    with pytest.raises(ValueError, match="non-empty"):
        pmm_draw(np.array([0.0]), np.array([]), np.array([]), k=1, rng=rng)


def test_pmm_draw_rejects_mismatched_donor_lengths():
    rng = np.random.default_rng(seed=0)
    with pytest.raises(ValueError, match="equal length"):
        pmm_draw(np.array([0.0]), np.array([0.0, 1.0]), np.array([10.0]), k=1, rng=rng)


def test_pmm_draw_rejects_non_finite_recipient_score():
    rng = np.random.default_rng(seed=0)
    with pytest.raises(ValueError, match="finite"):
        pmm_draw(np.array([np.nan]), np.array([0.0]), np.array([10.0]), k=1, rng=rng)


def test_pmm_draw_returns_float64_values_for_integer_donors():
    donor_values = np.array([10, 11], dtype=np.int64)
    result = pmm_draw(
        np.array([0.0]),
        np.array([0.0, 1.0]),
        donor_values,
        k=1,
        rng=np.random.default_rng(seed=0),
    )
    assert result.values.dtype == np.float64


def test_pmm_draw_rejects_non_integer_k():
    rng = np.random.default_rng(seed=0)
    with pytest.raises(TypeError, match="k must be an integer"):
        pmm_draw(
            np.array([0.0]),
            np.array([0.0]),
            np.array([1.0]),
            k=1.5,  # ty: ignore[invalid-argument-type]
            rng=rng,
        )


def test_pmm_draw_rejects_negative_exclusion_index():
    rng = np.random.default_rng(seed=0)
    with pytest.raises(ValueError, match="outside"):
        pmm_draw(
            np.array([0.0]),
            np.array([0.0, 1.0]),
            np.array([10.0, 11.0]),
            k=1,
            rng=rng,
            exclude=[[-1]],
        )


def test_pmm_draw_rejects_out_of_range_exclusion_index():
    rng = np.random.default_rng(seed=0)
    with pytest.raises(ValueError, match="outside"):
        pmm_draw(
            np.array([0.0]),
            np.array([0.0, 1.0]),
            np.array([10.0, 11.0]),
            k=1,
            rng=rng,
            exclude=[[5]],
        )


def test_pmm_draw_rejects_fractional_exclusion_index():
    rng = np.random.default_rng(seed=0)
    with pytest.raises(ValueError, match="integer donor indices"):
        pmm_draw(
            np.array([0.0]),
            np.array([0.0, 1.0]),
            np.array([10.0, 11.0]),
            k=1,
            rng=rng,
            exclude=[[0.9]],  # ty: ignore[invalid-argument-type]
        )


def test_pmm_draw_reweights_donors_so_pool_mean_is_not_the_drawn_mean():
    """Recipient-score matching makes the drawn mean differ from the raw donor mean.

    With all recipients scored at the far donor, every draw returns that donor's value,
    so the realised mean is that value -- not the unweighted mean of the donor pool.
    """
    donor_scores = np.array([0.0, 10.0])
    donor_residuals = np.array([-100.0, 1_000.0])
    recipient_scores = np.full(100, 10.0)
    result = pmm_draw(
        recipient_scores,
        donor_scores,
        donor_residuals,
        k=1,
        rng=np.random.default_rng(0),
    )
    assert result.values.mean() == 1_000.0
    assert donor_residuals.mean() == 450.0


def test_pmm_draw_rejects_boolean_exclusion_index():
    rng = np.random.default_rng(seed=0)
    with pytest.raises(ValueError, match="not booleans"):
        pmm_draw(
            np.array([0.0]),
            np.array([0.0, 1.0]),
            np.array([10.0, 11.0]),
            k=1,
            rng=rng,
            exclude=[[True]],
        )
