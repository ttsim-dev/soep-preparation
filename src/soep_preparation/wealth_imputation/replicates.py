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

from collections.abc import Mapping, Sequence

import numpy as np
import pandas as pd


def impute_replicates(  # noqa: PLR0913 -- keyword-only run + replicate settings
    modules: Mapping[str, pd.DataFrame],
    *,
    n_replicates: int,
    base_seed: int,
    transport_log_scale: float,
    total_scale: float,
    n_draws: int,
    k: int,
) -> pd.DataFrame:
    """Build `n_replicates` complete projection replicates of 2022 net wealth.

    Each replicate is one bootstrap refit (parameter uncertainty) plus one systematic
    transport shock (transport uncertainty) on top of the donor draws, so the spread
    across the returned columns prices those layers jointly. The columns are generic
    `draw_i` totals; mapping a released subset onto the DIW `a`-`e` names and attaching
    the projection metadata is a separate assembly step.

    Args:
        modules: Cleaned SOEP modules passed through to `run_imputation`.
        n_replicates: Number of replicates to draw (must be positive).
        base_seed: Seed anchoring the per-replicate bootstrap/draw seeds and the
            transport shocks.
        transport_log_scale: Transport-shock scale on the asinh axis; `0.0` disables
            the transport layer.
        total_scale: Positive asinh knee (euros) for the transport shock.
        n_draws: Donor draws per replicate, forwarded to `run_imputation`.
        k: Nearest-donor count, forwarded to `run_imputation`.

    Returns:
        A frame with `hh_id` and one `draw_i` net-wealth total column per replicate.

    Raises:
        ValueError: If `n_replicates` is not positive.

    """
    # Lazy import: `impute` imports `bayesian_bootstrap_weights` from this module, so a
    # top-level import here would form a cycle.
    from soep_preparation.wealth_imputation import impute  # noqa: PLC0415

    if n_replicates <= 0:
        msg = f"n_replicates must be positive, got {n_replicates}"
        raise ValueError(msg)
    deltas = draw_transport_shocks(
        n_replicates, log_scale=transport_log_scale, seed=base_seed
    )
    results = [
        impute.run_imputation(
            modules,
            n_draws=n_draws,
            seed=base_seed + index + 1,
            k=k,
            bootstrap_seed=base_seed + index + 1,
        )
        for index in range(n_replicates)
    ]
    frame = results[0].intervals[["hh_id"]].reset_index(drop=True)
    for index, result in enumerate(results):
        frame[f"draw_{index}"] = apply_transport_shock(
            result.intervals["point_estimate"].to_numpy(),
            float(deltas[index]),
            scale=total_scale,
        )
    return frame


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


def apply_transport_shock(
    totals: np.ndarray, delta: float, *, scale: float
) -> np.ndarray:
    """Shift signed household totals by `delta` on the `asinh(total / scale)` axis.

    Net wealth is signed, so a multiplicative `exp(delta)` shock is undefined on a
    negative total. Shifting on the asinh axis instead -- the same variance-stabilising
    axis the amount model fits on -- gives a monotone, signed analog of a proportional
    *level* shock: approximately multiplicative for wealth well outside
    `[-scale, scale]` and linear near zero. A positive `delta` shifts every total up
    (a richer 2022 than projected), a negative one down. The per-replicate shocks are
    mean-zero, so across replicates they add transport spread without a net bias. A
    household near zero net wealth can cross sign under a large shock -- an intended
    consequence of a level shift, not a magnitude scaler.

    Args:
        totals: Signed household net-wealth totals for one replicate.
        delta: The replicate's transport shock on the asinh axis.
        scale: Positive total scale setting the asinh knee (euros).

    Returns:
        The shocked totals, same shape as `totals`.

    """
    if scale <= 0:
        msg = f"scale must be positive, got {scale}"
        raise ValueError(msg)
    axis = np.arcsinh(np.asarray(totals, dtype=float) / scale)
    return scale * np.sinh(axis + delta)
