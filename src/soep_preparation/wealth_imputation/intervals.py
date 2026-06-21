"""Assemble household-total intervals from the joint-draw distribution.

Each draw is one complete joint draw already aggregated to the household total, so an
interval is the empirical spread of those totals -- the median as point estimate and
the central-`level` quantiles as bounds. Intervals are formed on the household-total
distribution directly; component interval endpoints are **never** summed, which would
ignore the cross-component dependence carried in each joint draw. Quantiles run in
`float64` and the helper fails closed on invalid input.
"""

import numpy as np
import pandas as pd

_DRAW_COLUMNS = ("hh_id", "survey_year", "household_total_draw")
_HOUSEHOLD_KEY_COLUMNS = ["hh_id", "survey_year"]


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
