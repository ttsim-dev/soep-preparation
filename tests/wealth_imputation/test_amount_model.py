"""Behavior of the amount model wrapper."""

import numpy as np
import pytest

from soep_preparation.wealth_imputation.amount_model import AmountModel

_SCALE = 1000.0


def _asinh_linear_data() -> tuple[np.ndarray, np.ndarray]:
    """Amounts whose asinh/scale transform is linear in a single feature."""
    feature = np.linspace(-2.0, 2.0, 40)
    observed = _SCALE * np.sinh(feature)
    return feature.reshape(-1, 1), observed


def test_amount_model_prediction_increases_with_the_feature():
    """A higher feature value yields a larger predicted euro amount."""
    features, observed = _asinh_linear_data()
    model = AmountModel.fit(features, observed, scale=_SCALE)
    predictions = model.predict(np.array([[-1.0], [1.0]]))
    assert predictions[1] > predictions[0]


def test_amount_model_recovers_observed_amounts_when_asinh_relation_is_linear():
    """A perfectly linear asinh relation is recovered in euros within tolerance."""
    features, observed = _asinh_linear_data()
    model = AmountModel.fit(features, observed, scale=_SCALE)
    predictions = model.predict(features)
    np.testing.assert_allclose(predictions, observed, rtol=1e-6)


@pytest.mark.parametrize("scale", [0.0, -1.0, np.inf])
def test_amount_model_rejects_invalid_scale(scale: float):
    """A non-positive or non-finite component scale fails closed."""
    features, observed = _asinh_linear_data()
    with pytest.raises(ValueError, match="scale"):
        AmountModel.fit(features, observed, scale=scale)


def test_amount_model_rejects_mismatched_lengths():
    """A target with a different row count than the design matrix fails closed."""
    features, observed = _asinh_linear_data()
    with pytest.raises(ValueError, match="same number of rows"):
        AmountModel.fit(features, observed[:-1], scale=_SCALE)
