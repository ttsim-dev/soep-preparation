"""Tests for the multiply-imputed 2022 wealth replicate engine."""

import numpy as np
import pytest

from soep_preparation.wealth_imputation.replicates import (
    bayesian_bootstrap_weights,
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
