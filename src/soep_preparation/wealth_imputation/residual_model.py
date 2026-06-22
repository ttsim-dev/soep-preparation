"""Residual model: allocate the unmodelled-components residual via OLS on the design.

The five SOEP-Core wealth components leave a residual to the official household total --
chiefly business assets and other real estate, which appear only inside the total. This
wraps a scikit-learn `LinearRegression` on the *raw signed* residual (it is negative
when modelled components exceed the official total, e.g. a heavily indebted household),
so the residual is allocated to the households whose covariates predict it rather than
spread as a flat population mean. OLS keeps the fitted residuals' mean equal to the
training mean, so the aggregate is redistributed, not inflated.
"""

from dataclasses import dataclass

import numpy as np
from sklearn.linear_model import LinearRegression


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
    """A fitted OLS model of the signed unmodelled-components residual."""

    estimator: LinearRegression
    """The fitted scikit-learn linear regression on the raw signed residual."""

    @classmethod
    def fit(cls, features: np.ndarray, residual: np.ndarray) -> ResidualModel:
        """Fit the residual model on the signed training residual.

        Args:
            features: Design matrix, shape `(n_rows, n_features)`, all finite.
            residual: Signed euro residual per row (`official - modelled`), all finite.

        Returns:
            A fitted `ResidualModel`.

        Raises:
            ValueError: On a non-2-D design matrix, mismatched lengths, or non-finite
                inputs.
        """
        _fail_if_training_data_invalid(features, residual)
        estimator = LinearRegression()
        estimator.fit(features, residual)
        return cls(estimator=estimator)

    def predict(self, features: np.ndarray) -> np.ndarray:
        """Return the predicted signed residual for each row of `features` as float64.

        Args:
            features: Design matrix with the same column layout used in `fit`.

        Returns:
            Predicted signed euro residuals, shape `(n_rows,)`.

        Raises:
            ValueError: On a non-2-D design matrix or non-finite features.
        """
        _fail_if_features_invalid(features)
        return self.estimator.predict(features).astype("float64")
