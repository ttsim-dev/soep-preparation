"""Tests for the multiply-imputed 2022 wealth replicate engine."""

import numpy as np
import pytest

from soep_preparation.wealth_imputation.replicates import (
    apply_transport_shock,
    bayesian_bootstrap_weights,
    draw_transport_shocks,
    transport_log_scale_from_fold_errors,
)


def test_bayesian_bootstrap_weights_has_one_weight_per_unit() -> None:
    """The weight vector has exactly one entry per training unit."""
    weights = bayesian_bootstrap_weights(n_units=50, seed=0)
    assert weights.shape == (50,)


def test_bayesian_bootstrap_weights_sum_to_n_units() -> None:
    """Weights are scaled to average one, so they sum to the unit count."""
    weights = bayesian_bootstrap_weights(n_units=200, seed=0)
    np.testing.assert_allclose(weights.sum(), 200.0, rtol=1e-9)


def test_bayesian_bootstrap_weights_are_non_negative() -> None:
    """A frequency weight can be zero but never negative."""
    weights = bayesian_bootstrap_weights(n_units=200, seed=3)
    assert weights.min() >= 0.0


def test_bayesian_bootstrap_weights_are_reproducible_for_a_seed() -> None:
    """The same seed reproduces the identical weight vector."""
    first = bayesian_bootstrap_weights(n_units=100, seed=7)
    second = bayesian_bootstrap_weights(n_units=100, seed=7)
    np.testing.assert_array_equal(first, second)


def test_bayesian_bootstrap_weights_differ_across_seeds() -> None:
    """Different replicate seeds draw different weights (parameter uncertainty)."""
    first = bayesian_bootstrap_weights(n_units=100, seed=1)
    second = bayesian_bootstrap_weights(n_units=100, seed=2)
    assert not np.array_equal(first, second)


@pytest.mark.parametrize("bad_n", [0, -5])
def test_bayesian_bootstrap_weights_rejects_non_positive_unit_count(bad_n: int) -> None:
    """A replicate needs at least one unit to reweight."""
    with pytest.raises(ValueError, match="n_units"):
        bayesian_bootstrap_weights(n_units=bad_n, seed=0)


def test_transport_log_scale_is_rms_of_fold_errors() -> None:
    """The transport scale is the root-mean-square forward-prediction log error."""
    scale = transport_log_scale_from_fold_errors([0.1, -0.1, 0.2])
    np.testing.assert_allclose(scale, np.sqrt((0.01 + 0.01 + 0.04) / 3), rtol=1e-9)


def test_transport_log_scale_is_zero_for_perfect_folds() -> None:
    """No forward-prediction error implies no transport uncertainty."""
    assert transport_log_scale_from_fold_errors([0.0, 0.0]) == 0.0


def test_transport_log_scale_rejects_no_folds() -> None:
    """Calibration needs at least one rolling-origin fold error."""
    with pytest.raises(ValueError, match="fold"):
        transport_log_scale_from_fold_errors([])


def test_draw_transport_shocks_has_one_shock_per_replicate() -> None:
    """Each replicate receives exactly one systematic shock."""
    shocks = draw_transport_shocks(n_replicates=60, log_scale=0.3, seed=0)
    assert shocks.shape == (60,)


def test_draw_transport_shocks_are_zero_when_scale_is_zero() -> None:
    """A zero transport scale leaves every replicate unshifted."""
    shocks = draw_transport_shocks(n_replicates=20, log_scale=0.0, seed=0)
    np.testing.assert_array_equal(shocks, np.zeros(20))


def test_draw_transport_shocks_match_the_calibrated_scale() -> None:
    """Across many replicates the shock spread reproduces the calibrated log scale."""
    shocks = draw_transport_shocks(n_replicates=100_000, log_scale=0.25, seed=1)
    np.testing.assert_allclose(shocks.std(), 0.25, rtol=0.02)


def test_draw_transport_shocks_are_reproducible_for_a_seed() -> None:
    """The same seed reproduces the identical shock draws."""
    first = draw_transport_shocks(n_replicates=40, log_scale=0.2, seed=5)
    second = draw_transport_shocks(n_replicates=40, log_scale=0.2, seed=5)
    np.testing.assert_array_equal(first, second)


def test_draw_transport_shocks_reject_negative_scale() -> None:
    """A log scale is a standard deviation and cannot be negative."""
    with pytest.raises(ValueError, match="log_scale"):
        draw_transport_shocks(n_replicates=10, log_scale=-0.1, seed=0)


def test_apply_transport_shock_is_identity_at_zero_delta() -> None:
    """A zero shock leaves signed totals unchanged."""
    totals = np.array([-50_000.0, 0.0, 250_000.0])
    shocked = apply_transport_shock(totals, 0.0, scale=100_000.0)
    np.testing.assert_allclose(shocked, totals, atol=1e-6)


def test_apply_transport_shock_is_monotone_in_delta() -> None:
    """A larger shock maps a total to a larger value."""
    totals = np.array([250_000.0])
    low = apply_transport_shock(totals, 0.1, scale=100_000.0)[0]
    high = apply_transport_shock(totals, 0.5, scale=100_000.0)[0]
    assert high > low


def test_apply_transport_shock_shifts_a_positive_total_up_for_positive_delta() -> None:
    """A positive level shock makes a positive total larger."""
    shocked = apply_transport_shock(np.array([250_000.0]), 0.4, scale=100_000.0)
    assert shocked[0] > 250_000.0


def test_apply_transport_shock_shifts_a_negative_total_up_for_positive_delta() -> None:
    """A positive level shock moves an indebted total up toward zero, not down."""
    shocked = apply_transport_shock(np.array([-50_000.0]), 0.4, scale=100_000.0)
    assert shocked[0] > -50_000.0


def test_apply_transport_shock_rejects_non_positive_scale() -> None:
    """The asinh scale must be positive."""
    with pytest.raises(ValueError, match="scale"):
        apply_transport_shock(np.array([1.0]), 0.1, scale=0.0)
