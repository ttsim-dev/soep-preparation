"""Behavior of the unmodelled-components residual model."""

import numpy as np
import pytest

from soep_preparation.wealth_imputation.residual_model import ResidualModel


def _linear_residual_data() -> tuple[np.ndarray, np.ndarray]:
    """A residual that is an exact affine function of a single feature."""
    feature = np.linspace(-2.0, 2.0, 40)
    residual = 3000.0 * feature + 5000.0
    return feature.reshape(-1, 1), residual


def test_residual_model_recovers_linear_residual():
    """A perfectly affine residual relation is recovered within tolerance."""
    features, residual = _linear_residual_data()
    model = ResidualModel.fit(features, residual)
    np.testing.assert_allclose(model.predict(features), residual, atol=1e-6)


def test_residual_model_predicts_higher_for_larger_feature():
    """A larger feature value yields a larger predicted residual."""
    features, residual = _linear_residual_data()
    model = ResidualModel.fit(features, residual)
    predictions = model.predict(np.array([[-1.0], [1.0]]))
    assert predictions[1] > predictions[0]


def test_residual_model_recovers_negative_residual():
    """A negative residual (modelled wealth exceeds the official total) is recovered."""
    features = np.linspace(0.0, 1.0, 10).reshape(-1, 1)
    residual = np.full(10, -2500.0)
    model = ResidualModel.fit(features, residual)
    np.testing.assert_allclose(model.predict(features), residual, atol=1e-6)


def test_residual_model_rejects_mismatched_lengths():
    """A residual with a different row count than the design matrix fails closed."""
    features, residual = _linear_residual_data()
    with pytest.raises(ValueError, match="same number of rows"):
        ResidualModel.fit(features, residual[:-1])


def test_residual_model_rejects_non_finite_residual():
    """A non-finite residual entry fails closed."""
    features, residual = _linear_residual_data()
    residual[0] = np.nan
    with pytest.raises(ValueError, match="finite"):
        ResidualModel.fit(features, residual)
