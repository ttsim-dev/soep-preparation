"""Multiply-imputed 2022 wealth as a set of `a`-`e` projection replicates.

SOEP-Core V41 ships no 2022 wealth wave, so every 2022 cell is a forward projection
from the 2002-2017 donor waves. This module builds several complete replicates of that
projection and exposes them in the DIW `a`-`e` shape, so downstream code can combine
them with Rubin's rules. Each replicate integrates the uncertainty layers that the
single-value proxy in `impute` collapses:

- parameter uncertainty, via an approximate-Bayesian-bootstrap reweighting of the
  training units before each refit (`bayesian_bootstrap_weights`);
- donor-draw uncertainty, via the predictive-mean-matching draw the imputation already
  performs;
- transport uncertainty, via a per-replicate shock calibrated on rolling-origin
  forward-prediction error (the 2017-to-2022 direction cannot be observed, so this layer
  is a calibrated scenario, not a validated posterior).

Because the whole wave is imputed, the released `a`-`e` columns carry a metadata block
recording that they are a historical projection (`uses_observed_2022_wealth = false`)
and whether transport uncertainty is priced, so an analyst cannot silently treat 2022
as an ordinary observed-and-imputed wealth wave.
"""

import numpy as np


def bayesian_bootstrap_weights(n_units: int, *, seed: int) -> np.ndarray:
    """Draw approximate-Bayesian-bootstrap weights for one replicate.

    Samples a `Dirichlet(1, ..., 1)` weight vector over the training units and scales it
    so the weights average one. Used as `sample_weight` in a replicate's model refit, so
    that each replicate reflects a different plausible parameter draw (the approximate
    Bayesian bootstrap), rather than the single fixed fit the point-estimate proxy uses.

    Args:
        n_units: Number of training units to reweight (must be positive).
        seed: Replicate seed; the same seed reproduces the same weights.

    Returns:
        A length-`n_units` array of non-negative weights summing to `n_units`.

    """
    if n_units <= 0:
        msg = f"n_units must be positive, got {n_units}"
        raise ValueError(msg)
    rng = np.random.default_rng(seed=seed)
    weights = rng.dirichlet(np.ones(n_units))
    return weights * n_units
