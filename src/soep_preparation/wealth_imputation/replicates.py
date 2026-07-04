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

from collections.abc import Sequence

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


def transport_log_scale_from_fold_errors(fold_log_errors: Sequence[float]) -> float:
    """Calibrate the transport-shock scale from rolling-origin forward-prediction error.

    Each rolling-origin fold predicts a wealth wave from strictly earlier waves and
    yields a systematic aggregate log error `log(predicted / observed)` for that forward
    step. The transport scale is their root mean square -- the typical magnitude by
    which a forward projection misses, absorbing both a systematic bias and between-fold
    variation. With only a few wealth waves this is a thin, scenario-grade estimate, not
    a validated posterior standard deviation.

    Args:
        fold_log_errors: One systematic log error per rolling-origin fold.

    Returns:
        The root-mean-square forward-prediction log error (non-negative).

    """
    errors = np.asarray(fold_log_errors, dtype=float)
    if errors.size == 0:
        msg = "fold_log_errors must contain at least one fold"
        raise ValueError(msg)
    return float(np.sqrt(np.mean(errors**2)))


def draw_transport_shocks(
    n_replicates: int, *, log_scale: float, seed: int
) -> np.ndarray:
    """Draw one systematic transport shock per replicate.

    Each replicate receives a single multiplicative log shift
    `delta ~ Normal(0, log_scale)` applied to every household's projected total, so the
    shock is *systematic* within a replicate. Sharing it across households is
    deliberate: only a common shift makes the between-replicate variance price the
    2017-to-2022 transport uncertainty; a per-household iid shock would average out of
    any aggregate estimand and leave that uncertainty unpriced. The mean is zero -- the
    layer adds uncertainty without asserting a known bias-correction direction.

    Args:
        n_replicates: Number of replicates to shock.
        log_scale: Transport-shock standard deviation on the log scale (>= 0); a zero
            scale disables the layer.
        seed: Seed for the shock draws.

    Returns:
        A length-`n_replicates` array of log-scale shifts.

    """
    if log_scale < 0:
        msg = f"log_scale must be non-negative, got {log_scale}"
        raise ValueError(msg)
    rng = np.random.default_rng(seed=seed)
    return rng.normal(0.0, log_scale, size=n_replicates)
