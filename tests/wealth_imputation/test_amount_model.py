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


def test_predict_score_stays_finite_at_the_extrapolation_boundary():
    """The PMM matching score is finite where `sinh` of the prediction would overflow.

    The amount model is matched on the asinh axis. `predict_score` returns that axis
    directly, so a far-out-of-support feature gives a large but finite score, whereas
    back-transforming with `sinh` first overflows to `inf` and corrupts the match.
    """
    features, observed = _asinh_linear_data()
    model = AmountModel.fit(features, observed, scale=_SCALE)
    score = model.predict_score(np.array([[500.0]]))
    assert np.all(np.isfinite(score))


def test_predict_score_is_the_asinh_axis_of_the_euro_prediction():
    """In support, `predict_score` equals `asinh(predict / scale)`."""
    features, observed = _asinh_linear_data()
    model = AmountModel.fit(features, observed, scale=_SCALE)
    point = np.array([[0.5]])
    expected = np.arcsinh(model.predict(point) / _SCALE)
    np.testing.assert_allclose(model.predict_score(point), expected, rtol=1e-9)
