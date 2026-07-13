"""Behavior of the ownership-incidence model wrapper."""

import numpy as np
import pytest

from soep_preparation.wealth_imputation.ownership_model import OwnershipModel


def _separable_data() -> tuple[np.ndarray, np.ndarray]:
    """Ownership perfectly separated by a single feature at zero."""
    features = np.linspace(-5.0, 5.0, 40).reshape(-1, 1)
    owns = (features.ravel() > 0.0).astype(int)
    return features, owns


def test_ownership_model_probability_increases_with_the_separating_feature():
    """A higher feature value yields a higher fitted ownership probability."""
    features, owns = _separable_data()
    model = OwnershipModel.fit(features, owns, seed=0)
    probabilities = model.probability(np.array([[-4.0], [4.0]]))
    assert probabilities[1] > probabilities[0]


def test_ownership_model_probabilities_lie_in_the_unit_interval():
    """Predicted probabilities are valid probabilities."""
    features, owns = _separable_data()
    model = OwnershipModel.fit(features, owns, seed=0)
    probabilities = model.probability(features)
    assert np.all((probabilities >= 0.0) & (probabilities <= 1.0))


def test_ownership_model_is_deterministic_for_a_fixed_seed():
    """The same seed and data reproduce identical probabilities."""
    features, owns = _separable_data()
    first = OwnershipModel.fit(features, owns, seed=3).probability(features)
    second = OwnershipModel.fit(features, owns, seed=3).probability(features)
    np.testing.assert_array_equal(first, second)


def test_ownership_model_rejects_single_class_outcome():
    """Fitting an incidence model needs both ownership classes present."""
    features = np.linspace(-5.0, 5.0, 10).reshape(-1, 1)
    owns = np.ones(10, dtype=int)
    with pytest.raises(ValueError, match="both classes"):
        OwnershipModel.fit(features, owns, seed=0)


def test_ownership_model_rejects_non_binary_outcome():
    """A non-0/1 outcome fails closed."""
    features = np.linspace(-5.0, 5.0, 10).reshape(-1, 1)
    owns = np.full(10, 2, dtype=int)
    with pytest.raises(ValueError, match="binary"):
        OwnershipModel.fit(features, owns, seed=0)


def test_ownership_model_uniform_sample_weight_matches_unweighted():
    """Weights of one reproduce the unweighted fit (bootstrap identity case)."""
    features, owns = _separable_data()
    weighted = OwnershipModel.fit(
        features, owns, seed=0, sample_weight=np.ones(owns.shape[0])
    )
    unweighted = OwnershipModel.fit(features, owns, seed=0)
    np.testing.assert_allclose(
        weighted.probability(features), unweighted.probability(features), rtol=1e-9
    )


def test_ownership_model_sample_weight_raises_incidence_when_owners_upweighted():
    """Upweighting owners raises the fitted ownership probability at the boundary."""
    features, owns = _separable_data()
    weights = np.where(owns == 1, 5.0, 1.0)
    weighted = OwnershipModel.fit(features, owns, seed=0, sample_weight=weights)
    unweighted = OwnershipModel.fit(features, owns, seed=0)
    point = np.array([[0.0]])
    assert weighted.probability(point)[0] > unweighted.probability(point)[0]
