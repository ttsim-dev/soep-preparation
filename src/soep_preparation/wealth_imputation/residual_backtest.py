"""K-fold cross-fit of the signed reconciliation residual within the 2017 wave.

The signed reconciliation residual (`official_total - modelled_components`) is fit on a
single wave -- 2017, the only wave carrying the augmented official total `n011h` -- so
the out-of-fold backtest that trains on 2002-2012 never exercises it, and the
residual-inclusive total has no out-of-sample evidence. This module supplies that
evidence *within* 2017: a K-fold cross-fit that holds out each fold in turn, fits the
`ResidualModel` and the donor residual pool on the other folds, and PMM-draws the
held-out fold's residual.

It validates only **cross-sectional** residual prediction: whether a held-out
household's signed residual can be predicted from comparable households' residuals in
the same wave. It cannot test the 2017->2022 temporal transport the production scenario
relies on -- there is no second `n011h` wave to transport to -- so the
residual-inclusive total stays a labelled scenario, not a headline. The functions here
are pure (arrays in, aggregates out); the wave assembly and JSON write live in
`task_residual_backtest`.
"""

from dataclasses import dataclass

import numpy as np

from soep_preparation.wealth_imputation.donors import pmm_draw
from soep_preparation.wealth_imputation.residual_model import ResidualModel


@dataclass(frozen=True)
class CrossFitDraws:
    """Held-out residual draws from a K-fold cross-fit, aligned to the input rows."""

    draws: np.ndarray
    """Drawn signed residual per row per draw, shape `(n_rows, n_draws)`. Each row's
    draws come only from donors in the other folds (strict out-of-fold)."""
    fold: np.ndarray
    """Fold label per row, shape `(n_rows,)`; the fold in which the row was held out."""
    donor_distance: np.ndarray
    """Nearest-donor score distance per row (first draw), shape `(n_rows,)`. A
    diagnostic of how far each held-out row sits from its out-of-fold donor pool."""


def assign_folds(*, n_rows: int, n_folds: int, rng: np.random.Generator) -> np.ndarray:
    """Assign each row to one of `n_folds` balanced folds at random.

    Fold sizes differ by at most one row. The assignment is a shuffle of the cyclic
    label pattern `0, 1, ..., n_folds-1, 0, ...`, so no fold is empty as long as
    `n_rows >= n_folds`.

    Args:
        n_rows: Number of rows to partition.
        n_folds: Number of folds (`>= 2`, `<= n_rows`).
        rng: NumPy random generator.

    Returns:
        Fold label per row, shape `(n_rows,)`, values in `[0, n_folds)`.

    Raises:
        ValueError: If `n_folds < 2` or `n_folds > n_rows`.
    """
    if n_folds < 2:  # noqa: PLR2004 -- a cross-fit needs at least two folds
        msg = f"n_folds must be >= 2, got {n_folds}."
        raise ValueError(msg)
    if n_folds > n_rows:
        msg = f"n_folds ({n_folds}) cannot exceed n_rows ({n_rows})."
        raise ValueError(msg)
    labels = np.arange(n_rows) % n_folds
    rng.shuffle(labels)
    return labels


def out_of_fold_donor_indices(fold: np.ndarray, *, held_out_fold: int) -> np.ndarray:
    """Return the row indices that form the donor pool for one held-out fold.

    The donor pool for a held-out fold is exactly the rows in the *other* folds, so a
    held-out household's own residual can never enter its donor pool.

    Args:
        fold: Fold label per row.
        held_out_fold: The fold currently held out.

    Returns:
        Sorted row indices of the donor pool (all rows not in `held_out_fold`).
    """
    return np.flatnonzero(fold != held_out_fold)


def cross_fit_residual_draws(  # noqa: PLR0913 -- pure scorer with explicit knobs
    design: np.ndarray,
    observed_residual: np.ndarray,
    *,
    n_folds: int,
    k: int,
    n_draws: int,
    rng: np.random.Generator,
) -> CrossFitDraws:
    """Cross-fit the residual model within one wave and draw each held-out residual.

    For each fold: fit a `ResidualModel` on the out-of-fold rows, build the donor
    residual pool from those same rows, score the held-out rows, and PMM-draw `n_draws`
    residuals per held-out row from its nearest out-of-fold donors. Held-out rows are
    excluded from both the fit and the donor pool, so the cross-fit is strictly
    out-of-fold.

    Args:
        design: Residual-model design matrix, shape `(n_rows, n_features)`, all finite.
        observed_residual: Signed (deflated) residual per row, all finite.
        n_folds: Number of cross-fit folds.
        k: Nearest-donor count for PMM (clipped to the out-of-fold pool size).
        n_draws: Number of residual draws per held-out row.
        rng: NumPy random generator.

    Returns:
        A `CrossFitDraws` with the held-out draw matrix, fold labels, and nearest-donor
        distances.

    Raises:
        ValueError: On a non-2-D design, a length mismatch, or fewer rows than folds.
    """
    if design.ndim != 2:  # noqa: PLR2004 -- a design matrix is 2-D by definition
        msg = f"design must be a 2-D design matrix, got {design.ndim}-D."
        raise ValueError(msg)
    if design.shape[0] != observed_residual.shape[0]:
        msg = (
            "design and observed_residual must have the same number of rows, got "
            f"{design.shape[0]} and {observed_residual.shape[0]}."
        )
        raise ValueError(msg)
    n_rows = observed_residual.shape[0]
    fold = assign_folds(n_rows=n_rows, n_folds=n_folds, rng=rng)
    draws = np.empty((n_rows, n_draws), dtype="float64")
    donor_distance = np.empty(n_rows, dtype="float64")
    for held_out_fold in range(n_folds):
        held_out = np.flatnonzero(fold == held_out_fold)
        donors = out_of_fold_donor_indices(fold, held_out_fold=held_out_fold)
        model = ResidualModel.fit(design[donors], observed_residual[donors])
        donor_scores = model.predict(design[donors])
        donor_values = observed_residual[donors]
        recipient_scores = model.predict(design[held_out])
        fold_k = min(k, donors.size)
        for draw_index in range(n_draws):
            result = pmm_draw(
                recipient_scores,
                donor_scores,
                donor_values,
                fold_k,
                rng,
            )
            draws[held_out, draw_index] = result.values
            if draw_index == 0:
                donor_distance[held_out] = result.distances
    return CrossFitDraws(draws=draws, fold=fold, donor_distance=donor_distance)


def score_residual_cross_fit(
    observed_residual: np.ndarray,
    predicted_draws: np.ndarray,
    component_total: np.ndarray,
    *,
    level: float,
) -> dict:
    """Score cross-fit residual draws against the observed residual, aggregates only.

    The point prediction is the per-row median across draws; the draw band is the
    central `level` quantile interval of each row's draws. Every returned value is a
    scalar or a small dict of scalars -- no per-household row, no raw value -- so the
    scorecard is disclosure-safe.

    Args:
        observed_residual: Signed observed residual per row, all finite.
        predicted_draws: Cross-fit draw matrix, shape `(n_rows, n_draws)`.
        component_total: Modelled component total per row (the residual-free household
            total) used to report the residual's effect on the household total.
        level: Central level of the draw band (e.g. `0.9`).

    Returns:
        A dict with the row count, Spearman rank correlation of the median predicted
        residual against the observed residual, sign accuracy, median absolute error,
        draw-band coverage of the observed residual, and the effect on the household
        total. The household-total effect is reported two ways, which must not be
        conflated:

        - `total_with_median_predicted_residual`: the cross-section of each row's
          median-across-draws residual added to its component total -- a
          point-prediction summary that, like any per-row median, collapses tail mass.
        - `total_with_drawn_residual_across_draws`: each statistic computed within a
          draw across rows, then summarised across draws -- the residual-inclusive
          *scenario* distribution, which preserves the draw-level tail.
        - `total_with_observed_residual`: the observed-residual total, for reference.

    Raises:
        ValueError: On a length mismatch or non-finite inputs.
    """
    observed = _finite_1d(observed_residual, "observed_residual")
    totals = _finite_1d(component_total, "component_total")
    if predicted_draws.ndim != 2:  # noqa: PLR2004 -- draw matrix is 2-D
        msg = f"predicted_draws must be 2-D, got {predicted_draws.ndim}-D."
        raise ValueError(msg)
    if predicted_draws.shape[0] != observed.size:
        msg = (
            "predicted_draws and observed_residual must have the same number of rows, "
            f"got {predicted_draws.shape[0]} and {observed.size}."
        )
        raise ValueError(msg)
    if totals.size != observed.size:
        msg = (
            "component_total and observed_residual must have the same length, got "
            f"{totals.size} and {observed.size}."
        )
        raise ValueError(msg)
    point = np.median(predicted_draws, axis=1)
    tail = (1.0 - level) / 2.0
    lower = np.quantile(predicted_draws, tail, axis=1)
    upper = np.quantile(predicted_draws, 1.0 - tail, axis=1)
    within_band = (observed >= lower) & (observed <= upper)
    total_draws = totals[:, None] + predicted_draws
    return {
        "n": int(observed.size),
        "n_draws": int(predicted_draws.shape[1]),
        "rank_correlation": _rank_correlation(observed, point),
        "sign_accuracy": float(np.mean(np.sign(point) == np.sign(observed))),
        "median_abs_error": float(np.median(np.abs(point - observed))),
        "band_coverage": float(np.mean(within_band)),
        # Median-collapsed point summary: useful for rank/point accuracy, but it erases
        # the draw-level sign/tail mass (see `total_with_drawn_residual_across_draws`).
        "total_with_median_predicted_residual": _distribution(totals + point),
        # Residual-inclusive scenario distribution: each statistic within a draw across
        # rows, then summarised across draws, so the draw-level tail survives.
        "total_with_drawn_residual_across_draws": _distribution_across_draws(
            total_draws, level=level
        ),
        "total_with_observed_residual": _distribution(totals + observed),
    }


def _distribution_across_draws(total_draws: np.ndarray, *, level: float) -> dict:
    """Summarise each draw's household-total distribution, then across draws.

    `total_draws` is `(n_rows, n_draws)`; each column is one draw's residual-inclusive
    household totals. Every statistic is computed within a draw across households, then
    summarised by its mean and central-`level` band across draws -- so the draw-level
    residual tail (notably the negative share) is preserved, unlike the median-collapsed
    point summary.
    """
    per_draw = {
        "mean": np.mean(total_draws, axis=0),
        "negative_share": np.mean(total_draws < 0.0, axis=0),
        "p10": np.quantile(total_draws, 0.1, axis=0),
        "p50": np.quantile(total_draws, 0.5, axis=0),
        "p90": np.quantile(total_draws, 0.9, axis=0),
        "p99": np.quantile(total_draws, 0.99, axis=0),
    }
    tail = (1.0 - level) / 2.0
    return {
        name: {
            "mean": float(np.mean(values)),
            "lower": float(np.quantile(values, tail)),
            "upper": float(np.quantile(values, 1.0 - tail)),
        }
        for name, values in per_draw.items()
    }


def _rank_correlation(observed: np.ndarray, predicted: np.ndarray) -> float:
    """Return Spearman's rho (Pearson correlation of the value ranks)."""
    if observed.size < 2:  # noqa: PLR2004 -- correlation needs at least two rows
        return float("nan")
    observed_rank = _ranks(observed)
    predicted_rank = _ranks(predicted)
    if np.std(observed_rank) == 0.0 or np.std(predicted_rank) == 0.0:
        return float("nan")
    return float(np.corrcoef(observed_rank, predicted_rank)[0, 1])


def _ranks(values: np.ndarray) -> np.ndarray:
    """Return average ranks (ties shared), 1-based, as float64."""
    order = np.argsort(values, kind="stable")
    ranks = np.empty(values.size, dtype="float64")
    ranks[order] = np.arange(1, values.size + 1, dtype="float64")
    unique, inverse, counts = np.unique(values, return_inverse=True, return_counts=True)
    sums = np.zeros(unique.size, dtype="float64")
    np.add.at(sums, inverse, ranks)
    return (sums / counts)[inverse]


def _distribution(values: np.ndarray) -> dict:
    """Return a disclosure-safe summary of a household-total vector."""
    return {
        "mean": float(np.mean(values)),
        "p10": float(np.quantile(values, 0.1)),
        "p50": float(np.quantile(values, 0.5)),
        "p90": float(np.quantile(values, 0.9)),
        "p99": float(np.quantile(values, 0.99)),
    }


def _finite_1d(values: np.ndarray, name: str) -> np.ndarray:
    array = np.asarray(values, dtype="float64")
    if array.ndim != 1:
        msg = f"{name} must be 1-D, got {array.ndim}-D."
        raise ValueError(msg)
    if not np.all(np.isfinite(array)):
        msg = f"{name} must be finite (no NaN/inf)."
        raise ValueError(msg)
    return array
