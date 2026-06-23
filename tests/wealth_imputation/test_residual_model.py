"""Behavior of the two-part unmodelled-components residual model."""

import numpy as np
import pytest

from soep_preparation.wealth_imputation.residual_model import ResidualModel


def _two_part_data() -> tuple[np.ndarray, np.ndarray]:
    """A residual that is held only by high-feature rows, growing with the feature.

    Low-feature rows are non-owners with a (noisy) non-positive residual; high-feature
    rows own unmodelled wealth in increasing amounts -- the concentrated, mostly-zero
    shape of business and other-real-estate wealth.
    """
    feature = np.linspace(-2.0, 2.0, 40)
    residual = np.where(feature > 0.0, 1000.0 * feature, -500.0)
    return feature.reshape(-1, 1), residual


def test_residual_model_predictions_are_non_negative():
    """Predicted residuals never go below zero."""
    features, residual = _two_part_data()
    predictions = ResidualModel.fit(features, residual, seed=0).predict(features)
    assert np.all(predictions >= 0.0)


def test_residual_model_predicts_more_for_likely_owners():
    """A likely-owner row gets a larger expected residual than an unlikely one."""
    features, residual = _two_part_data()
    model = ResidualModel.fit(features, residual, seed=0)
    predictions = model.predict(np.array([[-1.5], [1.5]]))
    assert predictions[1] > predictions[0]


def test_residual_model_concentrates_mass_away_from_unlikely_owners():
    """A clearly-non-owner row gets a near-zero residual, not the population average."""
    features, residual = _two_part_data()
    model = ResidualModel.fit(features, residual, seed=0)
    assert model.predict(np.array([[-2.0]]))[0] < residual[residual > 0.0].mean()


def test_residual_model_rejects_mismatched_lengths():
    """A residual with a different row count than the design matrix fails closed."""
    features, residual = _two_part_data()
    with pytest.raises(ValueError, match="same number of rows"):
        ResidualModel.fit(features, residual[:-1], seed=0)


def test_residual_model_rejects_non_finite_residual():
    """A non-finite residual entry fails closed."""
    features, residual = _two_part_data()
    residual[0] = np.nan
    with pytest.raises(ValueError, match="finite"):
        ResidualModel.fit(features, residual, seed=0)
