"""Ownership-incidence model: a thin, fail-closed logistic-regression boundary.

Wraps a scikit-learn `LogisticRegression` behind a small typed interface so the rest
of the harness depends on `fit` / `probability`, not on estimator internals. The
returned probability is `P(owns)` for each row and feeds the ownership step of
`value_generator.draw_component`. Real covariates are wired in the (deferred) data
stage; the wrapper itself is trained and tested on in-memory arrays.
"""

from dataclasses import dataclass

import numpy as np
from sklearn.linear_model import LogisticRegression


def _fail_if_training_data_invalid(features: np.ndarray, owns: np.ndarray) -> None:
    if features.ndim != 2:  # noqa: PLR2004 -- a design matrix is 2-D by definition
        msg = f"features must be a 2-D design matrix, got {features.ndim}-D."
        raise ValueError(msg)
    if owns.ndim != 1:
        msg = f"owns must be 1-D, got {owns.ndim}-D."
        raise ValueError(msg)
    if features.shape[0] != owns.shape[0]:
        msg = (
            "features and owns must have the same number of rows, got "
            f"{features.shape[0]} and {owns.shape[0]}."
        )
        raise ValueError(msg)
    if not np.all(np.isfinite(features)):
        msg = "features must be finite (no NaN/inf)."
        raise ValueError(msg)
    unique = set(np.unique(owns).tolist())
    if not unique <= {0, 1}:
        msg = f"owns must be binary 0/1, got values {sorted(unique)}."
        raise ValueError(msg)
    if unique != {0, 1}:
        msg = "owns must contain both classes (0 and 1) to fit an incidence model."
        raise ValueError(msg)


@dataclass(frozen=True)
class OwnershipModel:
    """A fitted ownership-incidence classifier."""

    estimator: LogisticRegression
    """The fitted scikit-learn logistic-regression estimator."""

    @classmethod
    def fit(
        cls, features: np.ndarray, owns: np.ndarray, *, seed: int
    ) -> OwnershipModel:
        """Fit the incidence model on observed ownership outcomes.

        Args:
            features: Design matrix, shape `(n_rows, n_features)`, all finite.
            owns: Binary ownership outcome per row (`0`/`1`), both classes present.
            seed: Random seed for the estimator's deterministic solver state.

        Returns:
            A fitted `OwnershipModel`.

        Raises:
            ValueError: On a non-2-D design matrix, mismatched lengths, non-finite
                features, or a non-binary / single-class outcome.
        """
        _fail_if_training_data_invalid(features, owns)
        estimator = LogisticRegression(random_state=seed)
        estimator.fit(features, owns)
        return cls(estimator=estimator)

    def probability(self, features: np.ndarray) -> np.ndarray:
        """Return `P(owns)` for each row of `features` as float64.

        Args:
            features: Design matrix with the same column layout used in `fit`.

        Returns:
            Ownership probabilities, shape `(n_rows,)`, in `[0, 1]`.

        Raises:
            ValueError: On a non-2-D design matrix or non-finite features.
        """
        if features.ndim != 2:  # noqa: PLR2004 -- a design matrix is 2-D by definition
            msg = f"features must be a 2-D design matrix, got {features.ndim}-D."
            raise ValueError(msg)
        if not np.all(np.isfinite(features)):
            msg = "features must be finite (no NaN/inf)."
            raise ValueError(msg)
        return self.estimator.predict_proba(features)[:, 1].astype("float64")
