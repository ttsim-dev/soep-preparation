"""Fit per-component wealth models and build their draw configs.

For one component, training derives ownership from the observed amount (positive =
owner), fits the ownership-incidence and asinh-scaled amount models on the historical
waves, and packages predictions for the target-wave recipients plus the observed donor
pool into a `ComponentDrawConfig` for `simulate.simulate_household_totals`. The
component scale is the median positive amount, so the asinh knee tracks each
component's magnitude. Pure functions over arrays; the I/O lives in the probe/impute
tasks.
"""

from dataclasses import dataclass

import numpy as np

from soep_preparation.wealth_imputation.amount_model import AmountModel
from soep_preparation.wealth_imputation.components import CanonicalComponent
from soep_preparation.wealth_imputation.ownership_model import OwnershipModel
from soep_preparation.wealth_imputation.simulate import ComponentDrawConfig


@dataclass(frozen=True)
class ComponentModels:
    """The fitted ownership and amount models for one component, with its scale."""

    ownership_model: OwnershipModel
    """Incidence model predicting `P(owns)`."""
    amount_model: AmountModel
    """Amount model whose `predict_score` gives the asinh-axis PMM match for owners."""
    scale: float
    """The asinh component scale used by the amount model."""


def derive_ownership(values: np.ndarray) -> np.ndarray:
    """Return 1 where the component amount is positive (owner), else 0.

    Args:
        values: Observed component amounts (positive magnitudes).

    Returns:
        Binary ownership outcome per row as an integer array.
    """
    return (np.asarray(values, dtype="float64") > 0.0).astype(int)


def component_scale(values: np.ndarray) -> float:
    """Return the median strictly-positive amount, or 1.0 if none are positive.

    Args:
        values: Observed component amounts.

    Returns:
        A positive, finite asinh scale for the component.
    """
    positive = np.asarray(values, dtype="float64")
    positive = positive[positive > 0.0]
    if positive.size == 0:
        return 1.0
    return float(np.median(positive))


def fit_component_models(
    features: np.ndarray, values: np.ndarray, *, seed: int
) -> ComponentModels:
    """Fit the ownership and amount models for one component.

    Args:
        features: Training design matrix, shape `(n_rows, n_features)`.
        values: Observed component amounts per row (positive magnitudes).
        seed: Random seed for the ownership model.

    Returns:
        The fitted `ComponentModels` (ownership model, amount model, scale).

    Raises:
        ValueError: If ownership has a single class, or model inputs are invalid.
    """
    scale = component_scale(values)
    owns = derive_ownership(values)
    ownership_model = OwnershipModel.fit(features, owns, seed=seed)
    owner_mask = owns.astype(bool)
    amount_model = AmountModel.fit(
        features[owner_mask],
        np.asarray(values, dtype="float64")[owner_mask],
        scale=scale,
    )
    return ComponentModels(
        ownership_model=ownership_model, amount_model=amount_model, scale=scale
    )


def build_component_config(  # noqa: PLR0913
    component: CanonicalComponent,
    models: ComponentModels,
    recipient_features: np.ndarray,
    recipient_shares: np.ndarray,
    donor_features: np.ndarray,
    donor_values: np.ndarray,
    k: int,
) -> ComponentDrawConfig:
    """Predict for recipients and donors and assemble a `ComponentDrawConfig`.

    Args:
        component: The canonical component being configured.
        models: The fitted models for this component.
        recipient_features: Target-wave recipient design matrix.
        recipient_shares: Ownership share per recipient, in `[0, 1]`.
        donor_features: Donor design matrix (observed owners).
        donor_values: Donor observed euro amounts.
        k: Number of nearest eligible donors to sample from.

    Returns:
        A `ComponentDrawConfig` ready for `simulate_household_totals`.
    """
    return ComponentDrawConfig(
        component=component,
        ownership_prob=models.ownership_model.probability(recipient_features),
        ownership_share=np.asarray(recipient_shares, dtype="float64"),
        recipient_predicted=models.amount_model.predict_score(recipient_features),
        donor_predicted=models.amount_model.predict_score(donor_features),
        donor_observed=np.asarray(donor_values, dtype="float64"),
        scale=models.scale,
        k=k,
    )
