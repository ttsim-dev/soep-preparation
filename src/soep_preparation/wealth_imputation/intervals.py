"""Assemble household-total donor-spread bands from the joint-draw distribution.

Two summaries are exposed, for two different jobs:

- `household_total_intervals` collapses each household's draws to a point estimate (the
  median) and a central-`level` band. The median is the per-household point estimate for
  covariate use only -- it is **not** the cross-section of the predictive wealth
  distribution, and taking the cross-section of medians mechanically erases the zero and
  negative mass that any single complete draw carries.
- `distribution_across_draws` computes each distributional statistic (Gini, quantiles,
  top/zero/negative shares) **within** each complete draw and then summarises that
  statistic across draws. This is the correct predictive wealth distribution: it
  preserves the within-draw zero/negative mass that the cross-section of medians
  destroys.

Both are **conditional donor-randomisation spreads**, not calibrated predictive
intervals: they capture ownership/PMM draw variability holding the fitted models and
the chosen implicate, with the residual *model* fixed but its draw redrawn per draw, and
the components carry no modelled cross-component covariance (only shared-covariate
dependence). Quantiles run in `float64` and the helpers fail closed on invalid input.
"""

import numpy as np
import pandas as pd

from soep_preparation.wealth_imputation.wealth_dynamics import (
    wave_distribution_summary,
)

_DRAW_COLUMNS = ("hh_id", "survey_year", "household_total_draw")
_HOUSEHOLD_KEY_COLUMNS = ["hh_id", "survey_year"]
_DRAW_INDEX_COLUMN = "draw_index"


def _fail_if_columns_missing(frame: pd.DataFrame, name: str) -> None:
    missing = [col for col in _DRAW_COLUMNS if col not in frame.columns]
    if missing:
        msg = f"{name} is missing required columns: {missing}"
        raise ValueError(msg)


def _fail_if_level_invalid(level: float) -> None:
    if not np.isfinite(level) or not (0.0 < level < 1.0):
        msg = f"level must lie strictly in (0, 1), got {level}."
        raise ValueError(msg)


def household_total_intervals(
    draws: pd.DataFrame, *, level: float = 0.9
) -> pd.DataFrame:
    """Summarise per-household total draws into a point estimate and interval.

    Args:
        draws: Columns `hh_id`, `survey_year`, `household_total_draw`. Each row is one
            complete joint draw aggregated to the household total; multiple rows per
            household span the implicate / donor / incidence draws.
        level: Central coverage of the interval (e.g. `0.9` gives the 5th and 95th
            percentiles). Must lie strictly in `(0, 1)`.

    Returns:
        Columns `hh_id`, `survey_year`, `point_estimate`, `lower`, `upper` (float64),
        one row per household-year.

    Raises:
        ValueError: On missing columns, a `level` outside `(0, 1)`, or non-numeric /
            non-finite draws.
    """
    _fail_if_columns_missing(draws, "draws")
    _fail_if_level_invalid(level)
    values = pd.to_numeric(draws["household_total_draw"], errors="raise").astype(
        "float64"
    )
    if not np.isfinite(values.to_numpy()).all():
        msg = "household_total_draw must be finite (no NaN/inf)."
        raise ValueError(msg)
    alpha = (1.0 - level) / 2.0
    keyed = draws[_HOUSEHOLD_KEY_COLUMNS].assign(household_total_draw=values)
    grouped = keyed.groupby(_HOUSEHOLD_KEY_COLUMNS, observed=True)[
        "household_total_draw"
    ]
    summary = grouped.agg(
        point_estimate=lambda series: float(np.quantile(series, 0.5)),
        lower=lambda series: float(np.quantile(series, alpha)),
        upper=lambda series: float(np.quantile(series, 1.0 - alpha)),
    ).reset_index()
    for column in ("point_estimate", "lower", "upper"):
        summary[column] = summary[column].astype("float64")
    return summary


def distribution_across_draws(
    draws: pd.DataFrame, *, level: float = 0.9
) -> dict[str, dict[str, float]]:
    """Summarise the predictive wealth distribution within draws, then across draws.

    For each complete draw -- the cross-section of one household total per household --
    every distributional statistic is computed on that draw's full set of household
    totals (so its zero and negative mass survive), and the statistic is then summarised
    across the draws. This is the predictive wealth distribution; the cross-section of
    per-household medians from `household_total_intervals` is **not**, because the
    median of a household's draws is rarely zero even when every draw has many zeros.

    Draw membership is the within-household draw position: the `i`-th row of each
    household belongs to draw `i`, matching how `simulate_household_totals` appends one
    household-total row per household per draw. Every draw must therefore cover the same
    set of households.

    Args:
        draws: Columns `hh_id`, `survey_year`, `household_total_draw`; multiple rows per
            household, one per complete draw.
        level: Central level for the across-draw spread of each statistic.

    Returns:
        A dict keyed by statistic name (`mean`, `gini`, `top10_share`, `top1_share`,
        `zero_share`, `negative_share`, and `p1`..`p99`), each mapping to a dict
        with the across-draw `mean`, `lower`, and `upper` of that statistic.

    Raises:
        ValueError: On missing columns, a `level` outside `(0, 1)`, non-finite draws, or
            draws whose households are not balanced across draws.
    """
    _fail_if_columns_missing(draws, "draws")
    _fail_if_level_invalid(level)
    values = pd.to_numeric(draws["household_total_draw"], errors="raise").astype(
        "float64"
    )
    if not np.isfinite(values.to_numpy()).all():
        msg = "household_total_draw must be finite (no NaN/inf)."
        raise ValueError(msg)
    keyed = draws[_HOUSEHOLD_KEY_COLUMNS].assign(household_total_draw=values)
    keyed[_DRAW_INDEX_COLUMN] = keyed.groupby(
        _HOUSEHOLD_KEY_COLUMNS, observed=True
    ).cumcount()
    counts_per_draw = keyed.groupby(_DRAW_INDEX_COLUMN, observed=True).size()
    if counts_per_draw.min() != counts_per_draw.max():
        msg = "every draw must cover the same set of households."
        raise ValueError(msg)
    per_draw_stats = [
        _draw_statistics(group["household_total_draw"].to_numpy(dtype="float64"))
        for _, group in keyed.groupby(_DRAW_INDEX_COLUMN, observed=True)
    ]
    stats_frame = pd.DataFrame(per_draw_stats)
    alpha = (1.0 - level) / 2.0
    return {
        statistic: {
            "mean": float(stats_frame[statistic].mean()),
            "lower": float(np.quantile(stats_frame[statistic], alpha)),
            "upper": float(np.quantile(stats_frame[statistic], 1.0 - alpha)),
        }
        for statistic in stats_frame.columns
    }


def _draw_statistics(values: np.ndarray) -> dict[str, float]:
    """Return one complete draw's distributional statistics as a flat dict."""
    summary = wave_distribution_summary(values)
    flat = {
        name: float(value)
        for name, value in summary.items()
        if name not in {"quantiles", "n"}
    }
    flat.update({name: float(value) for name, value in summary["quantiles"].items()})
    return flat
