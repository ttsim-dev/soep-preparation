"""Residual model: a signed matching score for the unmodelled-components residual.

The residual to the official total -- chiefly business and other real estate, net of any
omitted liabilities -- is *signed* (negative where modelled wealth meets or exceeds the
total) and concentrated. This fits an asinh-scaled linear regression on the signed
residual to produce a matching score. The residual itself is not plugged in
deterministically; it is drawn by predictive mean matching from observed donor residuals
in the simulation (`simulate_household_totals`), which preserves the sign and the
empirical distribution, avoids retransformation bias, and lets the residual contribute
genuine spread to the bands rather than a fixed shift.
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from soep_preparation.wealth_imputation.transforms import asinh_scaled


def _fail_if_features_invalid(features: np.ndarray) -> None:
    if features.ndim != 2:  # noqa: PLR2004 -- a design matrix is 2-D by definition
        msg = f"features must be a 2-D design matrix, got {features.ndim}-D."
        raise ValueError(msg)
    if not np.all(np.isfinite(features)):
        msg = "features must be finite (no NaN/inf)."
        raise ValueError(msg)


def _fail_if_training_data_invalid(features: np.ndarray, residual: np.ndarray) -> None:
    _fail_if_features_invalid(features)
    if residual.ndim != 1:
        msg = f"residual must be 1-D, got {residual.ndim}-D."
        raise ValueError(msg)
    if features.shape[0] != residual.shape[0]:
        msg = (
            "features and residual must have the same number of rows, got "
            f"{features.shape[0]} and {residual.shape[0]}."
        )
        raise ValueError(msg)
    if not np.all(np.isfinite(residual)):
        msg = "residual must be finite (no NaN/inf)."
        raise ValueError(msg)


@dataclass(frozen=True)
class ResidualModel:
    """A fitted signed matching-score model for the residual, with its asinh scale."""

    estimator: LinearRegression
    """The fitted linear regression on the asinh-scaled signed residual."""
    scale: float
    """The positive, finite scale used in the asinh transform."""

    @classmethod
    def fit(cls, features: np.ndarray, residual: np.ndarray) -> ResidualModel:
        """Fit the signed matching-score model on the training residual.

        Args:
            features: Design matrix, shape `(n_rows, n_features)`, all finite.
            residual: Signed euro residual per row (`official - modelled`), all finite.

        Returns:
            A fitted `ResidualModel`.

        Raises:
            ValueError: On invalid shapes, mismatched lengths, or non-finite inputs.
        """
        _fail_if_training_data_invalid(features, residual)
        magnitude = float(np.median(np.abs(residual)))
        scale = magnitude if magnitude > 0.0 else 1.0
        target = asinh_scaled(pd.Series(residual), scale).to_numpy()
        estimator = LinearRegression()
        estimator.fit(features, target)
        return cls(estimator=estimator, scale=scale)

    def predict(self, features: np.ndarray) -> np.ndarray:
        """Return the asinh-scale matching score per row as float64.

        The score is the linear prediction on the `asinh`-transformed axis, used only to
        rank donors for PMM. It is intentionally *not* back-transformed with `sinh`:
        PMM draws the observed (euro) donor residual, and `sinh` of an extrapolated
        linear prediction overflows, which would corrupt both the score ordering and any
        summary built from it.

        Args:
            features: Design matrix with the same column layout used in `fit`.

        Returns:
            Matching scores on the asinh axis, shape `(n_rows,)`.

        Raises:
            ValueError: On a non-2-D design matrix or non-finite features.
        """
        _fail_if_features_invalid(features)
        return self.estimator.predict(features).astype("float64")
