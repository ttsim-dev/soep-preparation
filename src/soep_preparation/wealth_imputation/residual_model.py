"""Residual model: a two-part allocator for the unmodelled-components residual.

The five SOEP-Core wealth components leave a residual to the official household total --
chiefly business assets and other real estate, which appear only inside the total and
are highly concentrated (most households hold none). A single linear fit smears that
mass across every household, inflating the middle of the distribution and manufacturing
spurious negatives. Instead, model it like a wealth component: a logistic incidence
model for *whether* a household holds unmodelled wealth (a positive residual), times an
asinh-scaled amount model for *how much*. The expected residual `P(owns) * amount` is
clipped at zero, so the allocation stays non-negative and concentrated where the
covariates predict it.
"""

from dataclasses import dataclass

import numpy as np

from soep_preparation.wealth_imputation.amount_model import AmountModel
from soep_preparation.wealth_imputation.ownership_model import OwnershipModel


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
    """A fitted two-part model of the signed unmodelled-components residual."""

    ownership: OwnershipModel
    """Logistic model for `P(residual > 0)` -- holding any unmodelled wealth."""
    amount: AmountModel
    """Asinh-scaled amount model fitted on the positive residuals only."""

    @classmethod
    def fit(
        cls, features: np.ndarray, residual: np.ndarray, *, seed: int
    ) -> ResidualModel:
        """Fit the incidence and amount parts on the signed training residual.

        A household with a positive residual is treated as holding unmodelled wealth;
        the amount part is fitted on those positive residuals only. Non-positive
        residuals (fit noise, where modelled wealth meets or exceeds the total) inform
        the incidence part as non-owners but do not enter the amount fit.

        Args:
            features: Design matrix, shape `(n_rows, n_features)`, all finite.
            residual: Signed euro residual per row (`official - modelled`), all finite,
                with both positive and non-positive values present.
            seed: Random seed for the incidence model's solver.

        Returns:
            A fitted `ResidualModel`.

        Raises:
            ValueError: On invalid shapes / lengths / non-finite inputs, or a residual
                without both owners and non-owners.
        """
        _fail_if_training_data_invalid(features, residual)
        owns = residual > 0.0
        ownership = OwnershipModel.fit(features, owns.astype("int64"), seed=seed)
        positive = residual[owns]
        scale = float(np.median(positive)) if positive.size else 1.0
        amount = AmountModel.fit(features[owns], positive, scale=scale)
        return cls(ownership=ownership, amount=amount)

    def predict(self, features: np.ndarray) -> np.ndarray:
        """Return the expected non-negative residual `P(owns) * amount` per row.

        Args:
            features: Design matrix with the same column layout used in `fit`.

        Returns:
            Expected signed euro residuals clipped at zero, shape `(n_rows,)`.

        Raises:
            ValueError: On a non-2-D design matrix or non-finite features.
        """
        _fail_if_features_invalid(features)
        expected = self.ownership.probability(features) * self.amount.predict(features)
        return np.clip(expected, 0.0, None).astype("float64")
