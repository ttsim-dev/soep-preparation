"""Behavior of the signed residual matching-score model."""

import numpy as np
import pytest

from soep_preparation.wealth_imputation.residual_model import ResidualModel


def _signed_linear_data() -> tuple[np.ndarray, np.ndarray]:
    """A signed residual increasing through zero with a single feature."""
    feature = np.linspace(-2.0, 2.0, 40)
    residual = 5000.0 * feature
    return feature.reshape(-1, 1), residual


def test_residual_model_score_increases_with_the_feature():
    """The matching score increases with a feature that drives the residual."""
    features, residual = _signed_linear_data()
    predictions = ResidualModel.fit(features, residual).predict(features)
    assert np.all(np.diff(predictions) > 0)


def test_residual_model_predicts_a_negative_score_for_low_feature():
    """A low-feature row gets a negative predicted residual (signed, not clipped)."""
    features, residual = _signed_linear_data()
    model = ResidualModel.fit(features, residual)
    assert model.predict(np.array([[-2.0]]))[0] < 0.0


def test_residual_model_rejects_mismatched_lengths():
    """A residual with a different row count than the design matrix fails closed."""
    features, residual = _signed_linear_data()
    with pytest.raises(ValueError, match="same number of rows"):
        ResidualModel.fit(features, residual[:-1])


def test_residual_model_rejects_non_finite_residual():
    """A non-finite residual entry fails closed."""
    features, residual = _signed_linear_data()
    residual[0] = np.nan
    with pytest.raises(ValueError, match="finite"):
        ResidualModel.fit(features, residual)
