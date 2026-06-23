"""Disclosure-safe wealth distribution and 5-year mobility summaries.

Turns household net wealth across the wealth waves (and the imputed 2022 proxy) into
aggregates that can leave the secure environment: per-wave distribution statistics
(quantiles, Gini, top shares) and quintile transition matrices across consecutive
5-year horizons. No row-level value is ever returned -- transition cells below
`min_cell` underlying observations are suppressed to `None`.

The cross-sectional distribution is taken over households (one net-wealth value each).
Mobility is taken over persons, each assigned their household's net wealth, because the
person identifier `p_id` is stable across waves whereas `hh_id` splits and reforms;
quintile ranks are defined on the full person cross-section of each wave, then movers
present in both waves are cross-tabulated.

Documented approximations: values are unweighted (SOEP design weights omitted); wealth
is per household, not equivalised; the 2022 leg is the imputed point estimate, so any
transition into 2022 is attenuated by imputation regression-to-the-mean. The Gini is
the mean-absolute-difference form and can exceed 1 when negative net wealth is present.
"""

from collections.abc import Sequence
from itertools import pairwise

import numpy as np
import pandas as pd

_QUANTILE_PROBABILITIES = {
    "p1": 0.01,
    "p5": 0.05,
    "p10": 0.10,
    "p25": 0.25,
    "p50": 0.50,
    "p75": 0.75,
    "p90": 0.90,
    "p95": 0.95,
    "p99": 0.99,
}
_TOP_DECILE = 0.10
_TOP_PERCENTILE = 0.01


def gini(values: np.ndarray) -> float:
    """Return the Gini coefficient via the mean absolute difference.

    Args:
        values: Net-wealth values, at least one, all finite.

    Returns:
        `sum_ij |x_i - x_j| / (2 n^2 mean)`. With negative net wealth present this can
        exceed 1; with a non-positive mean it is undefined and returned as `nan`.

    Raises:
        ValueError: On an empty or non-finite input.
    """
    array = _finite_1d(values, "values")
    mean = float(array.mean())
    if not np.isfinite(mean) or mean <= 0.0:
        return float("nan")
    # sum_ij |x_i - x_j| = 2 * sum_i (2i - n - 1) x_(i) for x sorted ascending, so the
    # Gini is computed in O(n log n) without ever materialising the n-by-n difference
    # matrix (which would exhaust memory at survey scale).
    sorted_values = np.sort(array)
    count = array.size
    rank = np.arange(1, count + 1)
    weighted_sum = float(np.sum((2 * rank - count - 1) * sorted_values))
    return weighted_sum / (count**2 * mean)


def top_share(values: np.ndarray, *, top_fraction: float) -> float:
    """Return the share of the total held by the richest `top_fraction` of holders.

    Args:
        values: Net-wealth values, at least one, all finite.
        top_fraction: Fraction in `(0, 1]` defining the top group; the group size is
            rounded up to at least one holder.

    Returns:
        The top group's summed value over the grand total, or `nan` if the total is
        non-positive.

    Raises:
        ValueError: On an empty/non-finite input or a `top_fraction` outside `(0, 1]`.
    """
    array = _finite_1d(values, "values")
    if not (0.0 < top_fraction <= 1.0):
        msg = f"top_fraction must lie in (0, 1], got {top_fraction}."
        raise ValueError(msg)
    total = float(array.sum())
    if total <= 0.0:
        return float("nan")
    count = max(1, int(np.ceil(top_fraction * array.size)))
    top = np.sort(array)[-count:]
    return float(top.sum() / total)


def quintile_ranks(values: pd.Series, *, n_groups: int = 5) -> pd.Series:
    """Rank values into `n_groups` groups, labelled 1 (lowest) upward, ties shared.

    Each value's group is `ceil(n_groups * percentile_rank)` using the *average* rank
    of ties, so identical wealth values always receive the identical group, independent
    of row order. Heavy ties (e.g. many households at exactly zero) therefore land
    together in one group rather than being split across a boundary by position -- the
    alternative would manufacture mobility between waves from row order alone.

    Args:
        values: Net-wealth values to rank.
        n_groups: Number of groups (5 for quintiles).

    Returns:
        Integer ranks in `1..n_groups`, aligned to `values.index`.
    """
    percentile = values.rank(method="average", pct=True)
    group = np.ceil(percentile * n_groups).clip(lower=1, upper=n_groups)
    return group.astype("int64")


def transition_counts(
    rank_from: pd.Series, rank_to: pd.Series, *, n_groups: int = 5
) -> np.ndarray:
    """Cross-tabulate origin against destination ranks into a count matrix.

    Args:
        rank_from: Origin-wave rank per mover, in `1..n_groups`.
        rank_to: Destination-wave rank per mover, aligned to `rank_from`.
        n_groups: Number of groups (matrix is `n_groups x n_groups`).

    Returns:
        A `(n_groups, n_groups)` float64 count matrix; row `i`, column `j` counts movers
        from group `i+1` to group `j+1`.
    """
    counts = np.zeros((n_groups, n_groups), dtype="float64")
    for origin, destination in zip(rank_from, rank_to, strict=True):
        counts[int(origin) - 1, int(destination) - 1] += 1.0
    return counts


def transition_probabilities(counts: np.ndarray) -> np.ndarray:
    """Row-normalise a count matrix into destination probabilities per origin group.

    Args:
        counts: A square count matrix.

    Returns:
        A matrix whose non-empty rows sum to 1; all-zero rows stay zero.
    """
    row_totals = counts.sum(axis=1, keepdims=True)
    safe_totals = np.where(row_totals == 0.0, 1.0, row_totals)
    return counts / safe_totals


def wave_distribution_summary(values: np.ndarray) -> dict:
    """Summarise one wave's net-wealth distribution into disclosure-safe scalars.

    Args:
        values: Household net-wealth values for the wave, all finite.

    Returns:
        A dict with the count, mean, a quantile grid, the Gini, the top-10% and top-1%
        shares, and the shares with negative and with zero net wealth.

    Raises:
        ValueError: On an empty or non-finite input.
    """
    array = _finite_1d(values, "values")
    quantiles = {
        name: float(np.quantile(array, probability))
        for name, probability in _QUANTILE_PROBABILITIES.items()
    }
    return {
        "n": int(array.size),
        "mean": float(array.mean()),
        "quantiles": quantiles,
        "gini": gini(array),
        "top10_share": top_share(array, top_fraction=_TOP_DECILE),
        "top1_share": top_share(array, top_fraction=_TOP_PERCENTILE),
        "negative_share": float((array < 0.0).mean()),
        "zero_share": float((array == 0.0).mean()),
    }


def build_dynamics_report(
    household_wealth: pd.DataFrame,
    roster: pd.DataFrame,
    *,
    waves: Sequence[int],
    n_groups: int = 5,
    min_cell: int = 30,
) -> dict:
    """Assemble the per-wave distribution and per-horizon mobility report.

    Args:
        household_wealth: Columns `hh_id`, `survey_year`, `net_wealth` covering every
            wave in `waves`.
        roster: Columns `p_id`, `hh_id`, `survey_year` linking persons to households.
        waves: Waves in chronological order; consecutive pairs form the horizons.
        n_groups: Number of mobility groups (5 for quintiles).
        min_cell: Transition cells below this many movers are suppressed to `None`.

    Returns:
        A nested, disclosure-safe dict with `metadata`, `distribution` (per wave) and
        `transitions` (per `"from->to"` horizon).
    """
    available = [
        wave for wave in waves if bool((household_wealth["survey_year"] == wave).any())
    ]
    without_data = [wave for wave in waves if wave not in available]
    distribution = {
        str(wave): wave_distribution_summary(
            household_wealth.loc[
                household_wealth["survey_year"] == wave, "net_wealth"
            ].to_numpy(dtype="float64")
        )
        for wave in available
    }
    person_rank = _person_quintile_ranks(
        household_wealth, roster, waves=available, n_groups=n_groups
    )
    transitions = {
        f"{wave_from}->{wave_to}": _horizon_transition(
            person_rank,
            wave_from=wave_from,
            wave_to=wave_to,
            n_groups=n_groups,
            min_cell=min_cell,
        )
        for wave_from, wave_to in pairwise(waves)
        if wave_from in available and wave_to in available
    }
    return {
        "metadata": {
            "wealth_concept": "household net overall wealth",
            "unit_distribution": "household",
            "unit_transitions": "person (assigned household net wealth)",
            "weighted": False,
            "n_groups": n_groups,
            "min_cell": min_cell,
            "waves": list(waves),
            "waves_without_data": without_data,
            "caveats": [
                "unweighted: no SOEP design/longitudinal weights applied",
                "not a population trend: sample composition and refresh samples vary "
                "by wave",
                "the imputed wave is a proxy; transitions into it are attenuated by "
                "regression to the mean",
            ],
        },
        "distribution": distribution,
        "transitions": transitions,
    }


def _person_quintile_ranks(
    household_wealth: pd.DataFrame,
    roster: pd.DataFrame,
    *,
    waves: Sequence[int],
    n_groups: int,
) -> pd.DataFrame:
    """Assign each person their household's net wealth and rank within each wave."""
    person_wealth = roster.merge(
        household_wealth, on=["hh_id", "survey_year"], how="inner"
    )
    person_wealth = person_wealth[person_wealth["survey_year"].isin(waves)]
    ranked = []
    for wave in waves:
        wave_rows = person_wealth.loc[person_wealth["survey_year"] == wave].copy()
        wave_rows["rank"] = quintile_ranks(wave_rows["net_wealth"], n_groups=n_groups)
        ranked.append(wave_rows[["p_id", "survey_year", "rank"]])
    return pd.concat(ranked, ignore_index=True)


def _horizon_transition(
    person_rank: pd.DataFrame,
    *,
    wave_from: int,
    wave_to: int,
    n_groups: int,
    min_cell: int,
) -> dict:
    """Cross-tabulate ranks of persons present in both waves of one horizon."""
    left = person_rank.loc[person_rank["survey_year"] == wave_from, ["p_id", "rank"]]
    right = person_rank.loc[person_rank["survey_year"] == wave_to, ["p_id", "rank"]]
    movers = left.merge(right, on="p_id", how="inner", suffixes=("_from", "_to"))
    counts = transition_counts(
        movers["rank_from"], movers["rank_to"], n_groups=n_groups
    )
    probabilities = transition_probabilities(counts)
    suppressed = counts < min_cell
    return {
        "n_persons": int(counts.sum()),
        "n_suppressed_cells": int(suppressed.sum()),
        "counts": _mask_suppressed(counts, suppressed),
        "probabilities": _mask_suppressed(probabilities, suppressed),
    }


def _mask_suppressed(matrix: np.ndarray, suppressed: np.ndarray) -> list:
    """Return a nested list with suppressed cells replaced by `None`."""
    return [
        [
            None if suppressed[i, j] else float(matrix[i, j])
            for j in range(matrix.shape[1])
        ]
        for i in range(matrix.shape[0])
    ]


def _finite_1d(values: np.ndarray, name: str) -> np.ndarray:
    array = np.asarray(values, dtype="float64")
    if array.ndim != 1:
        msg = f"{name} must be 1-D, got {array.ndim}-D."
        raise ValueError(msg)
    if array.size == 0:
        msg = f"{name} must be non-empty."
        raise ValueError(msg)
    if not np.all(np.isfinite(array)):
        msg = f"{name} must be finite (no NaN/inf)."
        raise ValueError(msg)
    return array
