"""Behavior of the per-component training and config-building helpers."""

import numpy as np

from soep_preparation.wealth_imputation.components import CanonicalComponent
from soep_preparation.wealth_imputation.simulate import ComponentDrawConfig
from soep_preparation.wealth_imputation.training import (
    build_component_config,
    component_scale,
    derive_ownership,
    fit_component_models,
)


def test_derive_ownership_flags_positive_values_as_owners():
    """Ownership is 1 where the component value is positive, else 0."""
    owns = derive_ownership(np.array([0.0, 100.0, 0.0, 50.0]))
    np.testing.assert_array_equal(owns, [0, 1, 0, 1])


def test_component_scale_is_the_median_of_positive_values():
    """The asinh scale is the median of the strictly positive amounts."""
    scale = component_scale(np.array([0.0, 100.0, 200.0, 300.0]))
    np.testing.assert_allclose(scale, 200.0, atol=1e-6)


def test_component_scale_falls_back_to_one_without_positive_values():
    """An all-zero component still yields a usable positive scale."""
    assert component_scale(np.array([0.0, 0.0])) == 1.0


def _training_data() -> tuple[np.ndarray, np.ndarray]:
    """Owners (value > 0) concentrated at high feature values."""
    feature = np.linspace(-3.0, 3.0, 40)
    values = np.where(feature > 0.0, 1000.0 * np.exp(feature), 0.0)
    return feature.reshape(-1, 1), values


def test_fit_component_models_ownership_probability_rises_with_the_feature():
    """The fitted ownership model assigns higher P(owns) to higher feature values."""
    features, values = _training_data()
    models = fit_component_models(features, values, seed=0)
    probabilities = models.ownership_model.probability(np.array([[-2.0], [2.0]]))
    assert probabilities[1] > probabilities[0]


def test_build_component_config_returns_aligned_config():
    """The built config carries one ownership probability per recipient."""
    features, values = _training_data()
    models = fit_component_models(features, values, seed=0)
    recipient_features = np.array([[1.0], [2.0]])
    config = build_component_config(
        component=CanonicalComponent.FINANCIAL_ASSETS,
        models=models,
        recipient_features=recipient_features,
        recipient_shares=np.array([1.0, 1.0]),
        donor_features=features[values > 0.0],
        donor_values=values[values > 0.0],
        k=5,
    )
    assert isinstance(config, ComponentDrawConfig)
    assert config.component is CanonicalComponent.FINANCIAL_ASSETS
    assert config.ownership_prob.shape == (2,)
    assert config.donor_observed.shape == config.donor_predicted.shape
