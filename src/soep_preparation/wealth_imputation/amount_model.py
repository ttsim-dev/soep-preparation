"""Amount model: predict component euro amounts via an asinh-scaled regression.

Wraps a scikit-learn `LinearRegression` fitted on the `asinh(y / s_c)`-transformed
amount, so the heavy right tail of wealth is variance-stabilised during fitting. The
public `predict` returns euro amounts (the asinh prediction is inverted with the same
component scale), which feed the PMM matching in `amounts.draw_amounts`. The wrapper is
a thin, fail-closed boundary; real covariates are wired in the deferred data stage.
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from soep_preparation.wealth_imputation.transforms import (
    asinh_scaled,
    inverse_asinh_scaled,
)


def _fail_if_training_data_invalid(
    features: np.ndarray, observed_amounts: np.ndarray
) -> None:
    if features.ndim != 2:  # noqa: PLR2004 -- a design matrix is 2-D by definition
        msg = f"features must be a 2-D design matrix, got {features.ndim}-D."
        raise ValueError(msg)
    if observed_amounts.ndim != 1:
        msg = f"observed_amounts must be 1-D, got {observed_amounts.ndim}-D."
        raise ValueError(msg)
    if features.shape[0] != observed_amounts.shape[0]:
        msg = (
            "features and observed_amounts must have the same number of rows, got "
            f"{features.shape[0]} and {observed_amounts.shape[0]}."
        )
        raise ValueError(msg)
    if not np.all(np.isfinite(features)):
        msg = "features must be finite (no NaN/inf)."
        raise ValueError(msg)
    if not np.all(np.isfinite(observed_amounts)):
        msg = "observed_amounts must be finite (no NaN/inf)."
        raise ValueError(msg)


@dataclass(frozen=True)
class AmountModel:
    """A fitted amount model with its asinh component scale."""

    estimator: LinearRegression
    """The fitted scikit-learn linear regression on the asinh-scaled target."""
    scale: float
    """The positive, finite component scale used in the asinh transform."""

    @classmethod
    def fit(
        cls, features: np.ndarray, observed_amounts: np.ndarray, *, scale: float
    ) -> AmountModel:
        """Fit the amount model on observed euro amounts.

        Args:
            features: Design matrix, shape `(n_rows, n_features)`, all finite.
            observed_amounts: Observed euro amounts per row, all finite.
            scale: Positive, finite component scale for the asinh transform.

        Returns:
            A fitted `AmountModel`.

        Raises:
            ValueError: On a non-2-D design matrix, mismatched lengths, non-finite
                inputs, or an invalid scale.
        """
        _fail_if_training_data_invalid(features, observed_amounts)
        target = asinh_scaled(pd.Series(observed_amounts), scale).to_numpy()
        estimator = LinearRegression()
        estimator.fit(features, target)
        return cls(estimator=estimator, scale=scale)

    def predict(self, features: np.ndarray) -> np.ndarray:
        """Return predicted euro amounts for each row of `features` as float64.

        Args:
            features: Design matrix with the same column layout used in `fit`.

        Returns:
            Predicted euro amounts, shape `(n_rows,)`.

        Raises:
            ValueError: On a non-2-D design matrix or non-finite features.
        """
        if features.ndim != 2:  # noqa: PLR2004 -- a design matrix is 2-D by definition
            msg = f"features must be a 2-D design matrix, got {features.ndim}-D."
            raise ValueError(msg)
        if not np.all(np.isfinite(features)):
            msg = "features must be finite (no NaN/inf)."
            raise ValueError(msg)
        transformed = self.estimator.predict(features)
        return inverse_asinh_scaled(pd.Series(transformed), self.scale).to_numpy()
