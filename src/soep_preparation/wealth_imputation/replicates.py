"""Component-only 2022 wealth as a set of projection replicates.

SOEP-Core V41 ships no 2022 wealth wave, so every 2022 cell is a forward projection
from the 2002-2017 donor waves. This module builds several complete replicates of that
projection -- the six-component net-wealth total, *not* the residual-inclusive total --
and releases them lettered `a`-`e`. The letters are exchangeable projection draws, not
DIW's own donor implicates, and the release is deliberately **not** Rubin-valid: the
metadata block records that so an analyst cannot silently treat 2022 as an ordinary
observed-and-imputed wealth wave. Each replicate integrates the uncertainty layers that
the single-value proxy in `impute` collapses:

- parameter uncertainty, via an approximate-Bayesian-bootstrap reweighting of the
  training units before each refit (`bayesian_bootstrap_weights`); this perturbs the
  fitted models, not the predictive-mean-matching donor pool, so donor-composition
  uncertainty is only partly captured;
- donor-draw uncertainty, via the single predictive-mean-matching draw each replicate
  performs (the replicates *are* the draws, so this is carried across `a`-`e`);
- transport uncertainty, via a per-replicate asinh-axis level shock whose scale is
  calibrated from the official aggregate's cross-wave log-growth dispersion
  (`transport_scale_from_official_aggregates`). The 2017-to-2022 direction cannot be
  observed, so this is a calibrated prior, not a validated posterior; the shock is
  mean-zero on the asinh axis but shifts euro-scale means, so it moves the level.
"""

from collections.abc import Mapping, Sequence
from typing import TypedDict

import numpy as np
import pandas as pd


class OfficialWealthAggregates(TypedDict):
    """Disclosure-safe official-wealth aggregates that calibrate the transport layer."""

    wave_aggregates: dict[int, float]
    """Summed official net-wealth total per wealth wave."""
    median_absolute_total: float
    """Median absolute total -- the asinh knee for the transport shock."""


class ImplicatesMetadata(TypedDict):
    """Provenance and validity guards stored with the released projection draws."""

    method: str
    n_internal_replicates: int
    n_released_draws: int
    transport_log_scale: float
    total_scale: float
    uses_observed_2022_wealth: bool
    transport_uncertainty_included: bool
    transport_posterior_validated: bool
    euro_scale_mean_neutral: bool
    component_only: bool
    residual_inclusive: bool
    donor_implicates_propagated: bool
    rubin_valid: bool
    distribution_calibrated: bool


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
    """Build `n_replicates` component-only projection replicates of 2022 net wealth.

    Each replicate is one bootstrap refit (parameter uncertainty) plus one systematic
    transport shock (transport uncertainty) on top of a single donor draw, so the spread
    across the returned columns prices those layers jointly. `n_draws` must be 1: the
    replicate *is* the draw, so donor-draw uncertainty is carried across replicates
    rather than averaged into a per-replicate median. The columns are generic `draw_i`
    totals; selecting and lettering a released subset is a separate assembly step.

    Args:
        modules: Cleaned SOEP modules passed through to `run_imputation`.
        n_replicates: Number of replicates to draw (must be positive).
        base_seed: Seed anchoring the per-replicate bootstrap/draw seeds and the
            transport shocks.
        transport_log_scale: Transport-shock scale on the asinh axis; `0.0` disables
            the transport layer.
        total_scale: Positive asinh knee (euros) for the transport shock.
        n_draws: Draws per replicate, forwarded to `run_imputation`; must be 1.
        k: Nearest-donor count, forwarded to `run_imputation`.

    Returns:
        A frame with `hh_id` and one `draw_i` net-wealth total column per replicate.

    Raises:
        ValueError: If `n_replicates` is not positive or `n_draws` is not 1.

    """
    # Lazy import: `impute` imports `bayesian_bootstrap_weights` from this module, so a
    # top-level import here would form a cycle.
    from soep_preparation.wealth_imputation import impute  # noqa: PLC0415

    if n_replicates <= 0:
        msg = f"n_replicates must be positive, got {n_replicates}"
        raise ValueError(msg)
    if n_draws != 1:
        # Each implicate is one predictive draw; `run_imputation` collapses multiple
        # draws to their median, which would average away the donor-draw uncertainty
        # the implicates are meant to carry.
        msg = f"each implicate is a single draw, so n_draws must be 1, got {n_draws}"
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


_IMPLICATE_LETTERS = "abcdefghijklmnopqrstuvwxyz"


def select_released_implicates(
    frame: pd.DataFrame,
    *,
    n_released: int = 5,
    name: str = "component_only_net_wealth_2022",
) -> pd.DataFrame:
    """Select the released component-only projection draws from a replicate frame.

    One column per released draw, keyed by `hh_id`. The columns are lettered `a`-`e`
    for a five-draw release, but they are exchangeable projection draws, not DIW's own
    donor implicates (see `build_implicates_metadata`). More replicates than released
    columns can be run to estimate the Monte-Carlo error (`replicate_mc_summary`); the
    released set is drawn evenly across the replicate index so it spans the full run
    rather than an arbitrary prefix.

    Args:
        frame: Engine output with `hh_id` and one `draw_i` total column per replicate.
        n_released: Number of implicates to release (five mirrors DIW).
        name: Column-name stem; each released column is `{name}_{letter}`.

    Returns:
        A frame with `hh_id` and `n_released` lettered net-wealth implicate columns.

    Raises:
        ValueError: If fewer than `n_released` replicates are available.

    """
    draw_columns = [
        column for column in frame.columns if str(column).startswith("draw_")
    ]
    if len(draw_columns) < n_released:
        msg = (
            f"n_released={n_released} exceeds the {len(draw_columns)} "
            "replicates available"
        )
        raise ValueError(msg)
    positions = np.linspace(0, len(draw_columns) - 1, n_released).round().astype(int)
    released = frame[["hh_id"]].reset_index(drop=True)
    for letter, position in zip(_IMPLICATE_LETTERS, positions, strict=False):
        released[f"{name}_{letter}"] = frame[draw_columns[position]].to_numpy()
    return released


def replicate_mc_summary(frame: pd.DataFrame) -> dict[str, float]:
    """Summarise the Monte-Carlo error of the aggregate across replicates.

    Reports how much the population-mean net wealth wobbles from replicate to replicate
    -- the Monte-Carlo error introduced by imputing with a finite number of replicates,
    estimated over every replicate the engine produced (not just the released subset).

    Args:
        frame: Engine output with `hh_id` and one `draw_i` total column per replicate.

    Returns:
        `n_replicates`, the mean aggregate, its between-replicate standard deviation,
        and the relative Monte-Carlo error (`sd / |mean|`, `nan` when the mean is
        ~zero).

    """
    draw_columns = [
        column for column in frame.columns if str(column).startswith("draw_")
    ]
    per_replicate_mean = np.array(
        [frame[column].to_numpy().mean() for column in draw_columns], dtype=float
    )
    aggregate_mean = float(per_replicate_mean.mean())
    between_replicate_sd = float(per_replicate_mean.std(ddof=0))
    relative_mc_error = (
        float("nan")
        if np.isclose(aggregate_mean, 0.0)
        else between_replicate_sd / abs(aggregate_mean)
    )
    return {
        "n_replicates": len(draw_columns),
        "aggregate_mean": aggregate_mean,
        "aggregate_between_replicate_sd": between_replicate_sd,
        "relative_mc_error": relative_mc_error,
    }


def transport_scale_from_official_aggregates(
    wave_aggregates: Mapping[int, float],
) -> float:
    """Calibrate the transport log-scale from official cross-wave aggregate levels.

    The size of a five-year forward wealth-level projection error is unobservable for
    the 2017-to-2022 step, because SOEP-Core V41 ships no 2022 wealth wave. As an
    empirical prior, use the dispersion of the official aggregate's own five-year
    log-growth across the observed wealth waves: how much the population wealth level
    has historically moved per step bounds how far the 2022 projection can
    systematically drift. The wealth waves are equally spaced (five years), so the
    consecutive log-growth steps are comparable.

    Args:
        wave_aggregates: Official population wealth aggregate per wealth-wave year (e.g.
            summed `w011h`), for at least three waves (two growth steps).

    Returns:
        The sample standard deviation of the consecutive log-growth steps -- the
        transport log-scale for `draw_transport_shocks`.

    Raises:
        ValueError: If fewer than three waves, or a non-positive aggregate.

    """
    minimum_waves = 3
    if len(wave_aggregates) < minimum_waves:
        msg = f"need at least {minimum_waves} waves to estimate a dispersion"
        raise ValueError(msg)
    years = sorted(wave_aggregates)
    levels = np.array([wave_aggregates[year] for year in years], dtype=float)
    if np.any(levels <= 0.0):
        msg = "official aggregates must be positive to take a logarithm"
        raise ValueError(msg)
    log_steps = np.diff(np.log(levels))
    return float(log_steps.std(ddof=1))


def robust_total_scale(totals: np.ndarray) -> float:
    """Return the median absolute net-wealth total, or 1.0 if that is not positive.

    Sets the asinh knee for the transport shock so it tracks the magnitude of the wealth
    distribution rather than a hard-coded euro figure.

    Args:
        totals: Signed household net-wealth totals.

    Returns:
        A positive, finite asinh scale.

    """
    magnitude = np.abs(np.asarray(totals, dtype="float64"))
    median = float(np.median(magnitude)) if magnitude.size else 0.0
    return median if median > 0.0 else 1.0


def official_wealth_aggregates(
    household_wealth: pd.DataFrame,
    *,
    total_column: str,
    waves: Sequence[int],
    wave_column: str = "survey_year",
    weight_column: str | None = None,
) -> OfficialWealthAggregates:
    """Aggregate the official household net-wealth total per wealth wave.

    With `weight_column`, each wave aggregate is the design-weighted total
    `Σ wᵢ · totalᵢ` (a population total, invariant to sample size and composition), and
    the knee is the weighted median absolute total. Without it, the aggregates are raw
    unweighted row sums -- sensitive to how many households each wave sampled -- so the
    resulting transport scale should be read as a sample-total-growth prior.

    Args:
        household_wealth: Cleaned household-wealth frame across waves.
        total_column: Column holding the official net-wealth total (e.g. `w011h`).
        waves: The wealth-wave years to aggregate.
        wave_column: Column identifying each row's survey year.
        weight_column: Optional household design-weight column; rows with a missing
            weight are dropped.

    Returns:
        `wave_aggregates` (weighted or raw total per wave, waves with no data omitted)
        and `median_absolute_total`.

    """
    totals = pd.to_numeric(household_wealth[total_column], errors="coerce")
    years = household_wealth[wave_column]
    weights = (
        pd.to_numeric(household_wealth[weight_column], errors="coerce")
        if weight_column is not None
        else None
    )
    wave_aggregates: dict[int, float] = {}
    for wave in waves:
        in_wave = years == wave
        if weights is None:
            wave_totals = totals[in_wave].dropna()
            if not wave_totals.empty:
                wave_aggregates[wave] = float(wave_totals.sum())
        else:
            valid = in_wave & totals.notna() & weights.notna()
            if valid.any():
                wave_aggregates[wave] = float((totals[valid] * weights[valid]).sum())
    in_waves = years.isin(waves)
    if weights is None:
        knee = robust_total_scale(totals[in_waves].dropna().to_numpy())
    else:
        valid = in_waves & totals.notna() & weights.notna()
        knee = _weighted_median_absolute(
            totals[valid].to_numpy(), weights[valid].to_numpy()
        )
    return {"wave_aggregates": wave_aggregates, "median_absolute_total": knee}


def _weighted_median_absolute(values: np.ndarray, weights: np.ndarray) -> float:
    """Return the weighted median of `|values|`, or 1.0 if no positive weight remains.

    The weighted median is the smallest absolute total at which the cumulative weight
    reaches half the total weight -- the robust, sample-composition-invariant analog of
    `robust_total_scale`'s asinh knee.
    """
    magnitude = np.abs(np.asarray(values, dtype="float64"))
    weight = np.asarray(weights, dtype="float64")
    total_weight = weight.sum()
    if magnitude.size == 0 or total_weight <= 0.0:
        return 1.0
    order = np.argsort(magnitude)
    cumulative = np.cumsum(weight[order])
    crossing = np.searchsorted(cumulative, total_weight / 2.0)
    median = float(magnitude[order][min(int(crossing), magnitude.size - 1)])
    return median if median > 0.0 else 1.0


def build_implicates_metadata(
    *,
    n_replicates: int,
    n_released: int,
    transport_log_scale: float,
    total_scale: float,
) -> ImplicatesMetadata:
    """Assemble the metadata guards recorded with the released projection draws.

    The guards keep an analyst from treating the release as ordinary
    observed-and-imputed wealth. Each flag records a way the object falls short of
    Rubin-valid SOEP wealth implicates:

    - `component_only` / `residual_inclusive`: the release is the six-component total,
      omitting the reconciliation residual (business, other real estate).
    - `transport_posterior_validated`: the transport scale is a calibrated prior, not a
      validated posterior (the 2017-to-2022 drift is unobservable).
    - `euro_scale_mean_neutral`: the asinh-axis shock is mean-zero on its own axis but
      shifts euro-scale means, so it moves the level, not just its uncertainty.
    - `donor_implicates_propagated`: DIW's donor implicates `b`-`e` are not threaded
      through the fits, so the `a`-`e` letters are exchangeable draws, not DIW worlds.
    - `rubin_valid`: false while any of the above hold.

    Args:
        n_replicates: Internal replicates the engine ran.
        n_released: Released projection draws.
        transport_log_scale: Calibrated transport shock scale on the asinh axis.
        total_scale: Asinh knee used for the transport shock.

    Returns:
        A JSON-serialisable metadata block.

    """
    return {
        "method": "component_only_projection_replicates",
        "n_internal_replicates": n_replicates,
        "n_released_draws": n_released,
        "transport_log_scale": float(transport_log_scale),
        "total_scale": float(total_scale),
        "uses_observed_2022_wealth": False,
        "transport_uncertainty_included": True,
        "transport_posterior_validated": False,
        "euro_scale_mean_neutral": False,
        "component_only": True,
        "residual_inclusive": False,
        "donor_implicates_propagated": False,
        "rubin_valid": False,
        "distribution_calibrated": False,
    }


def bayesian_bootstrap_weights(n_units: int, *, seed: int) -> np.ndarray:
    """Draw approximate-Bayesian-bootstrap weights for one replicate.

    Samples a `Dirichlet(1, ..., 1)` weight vector over the training units and scales it
    so the weights average one. Used as `sample_weight` in a replicate's model refit, so
    that each replicate reflects a different plausible parameter draw (the approximate
    Bayesian bootstrap), rather than the single fixed fit the point-estimate proxy uses.
    This reweights the fitted ownership and amount models only; the predictive-mean-
    matching donor pool and its nearest-neighbour selection are left unweighted, so
    donor-composition uncertainty is not fully captured.

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
    (a richer 2022 than projected), a negative one down. The shocks are mean-zero *on
    the asinh axis*, but the transform is convex in `delta`, so a symmetric shock is not
    mean-neutral on the euro scale: `E[sinh(x + delta)] = sinh(x) E[cosh(delta)]` with
    `E[cosh(delta)] > 1`, so positive totals drift up and negative totals down in
    expectation. The layer therefore moves the euro-scale level, not only its
    uncertainty. A household near zero net wealth can cross sign under a large shock --
    an intended consequence of a level shift, not a magnitude scaler.

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
