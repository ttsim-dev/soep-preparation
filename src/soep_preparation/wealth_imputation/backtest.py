"""Out-of-fold backtest: score imputed wealth against the observed wealth wave.

The proxy is validated by holding out the latest observed wealth wave (2017): fit on the
earlier waves, impute 2017 as if it were the target, and compare the imputed 2017
household totals with the held-out wave's completed-component totals. The target is the
six-component sum kept only where every component is present -- a completed-component
fidelity check, not raw-observed truth. All outputs are disclosure-safe aggregates --
distribution summaries, a quintile confusion matrix, rank accuracy, and band coverage --
never a row-level value.

Only the modelled component sum is backtested. The residual to the official total cannot
be validated this way: the augmented official total (`n011h`) exists only from 2017, so
there is no earlier outcome wave to fit a residual on, and the comparison target is the
completed component sum, not the official total.
"""

import numpy as np
import pandas as pd

from soep_preparation.wealth_imputation.intervals import distribution_across_draws
from soep_preparation.wealth_imputation.wealth_dynamics import (
    quintile_ranks,
    transition_counts,
    wave_distribution_summary,
)

_COMPARISON_COLUMNS = ("observed_total", "point_estimate", "lower", "upper")
_HH_KEYS = ["hh_id", "survey_year"]


def rank_correlation(observed: np.ndarray, predicted: np.ndarray) -> float:
    """Return the Spearman rank correlation between observed and predicted values.

    Args:
        observed: Observed values.
        predicted: Predicted values, aligned to `observed`.

    Returns:
        The Pearson correlation of the value ranks (i.e. Spearman's rho).
    """
    observed_rank = pd.Series(observed).rank().to_numpy()
    predicted_rank = pd.Series(predicted).rank().to_numpy()
    return float(np.corrcoef(observed_rank, predicted_rank)[0, 1])


def backtest_report(
    comparison: pd.DataFrame,
    *,
    imputed_draws: pd.DataFrame | None = None,
    level: float = 0.9,
    n_groups: int = 5,
) -> dict:
    """Score imputed household totals against the completed-component truth.

    Two imputed-distribution summaries are reported, because they answer different
    questions and are *not* interchangeable:

    - `imputed_distribution` is the cross-section of per-household **median** point
      estimates. It is the right basis for rank/quintile accuracy, but a household's
      median is rarely negative even when many of its draws are, so its negative/zero
      shares understate the predictive mass.
    - `imputed_distribution_across_draws` (only when `imputed_draws` is given) is the
      **draw-level** distribution over the same households, computed within each
      complete draw then averaged across draws. This is the apples-to-apples counterpart
      of the production `distribution_across_draws`; use it (not the median
      cross-section) to compare negative/zero shares and tails against a production run.

    Args:
        comparison: Columns `observed_total`, `point_estimate`, `lower`, `upper`, plus
            `hh_id`, `survey_year`; one row per household in the held-out wave. The
            target is the **completed six-component sum**, not raw-observed truth.
        imputed_draws: Optional component-only draw table (`hh_id`, `survey_year`,
            `household_total_draw`) for the held-out wave. Restricted to the households
            in `comparison` to form the draw-level imputed distribution.
        level: Central level for the across-draw spread of each draw-level statistic.
        n_groups: Number of mobility groups for the rank comparison (5 for quintiles).

    Returns:
        A dict with the observed and imputed (median) distribution summaries, the
        draw-level imputed distribution (`imputed_distribution_across_draws`, `None` if
        `imputed_draws` is not given), the quintile confusion counts (rows = observed
        quintile, columns = predicted quintile), exact quintile accuracy, mean absolute
        quintile error, the rank correlation, the band coverage, and the median absolute
        level error. No row-level value is included.

        `band_coverage` is the fraction of completed-component totals that fall inside
        the imputed donor-spread band. The band is a conditional donor-spread quantile,
        not a calibrated predictive interval, so a high coverage is *not* evidence of
        predictive calibration -- it only says the conditional donor spread happens to
        bracket the values at this rate.

    Raises:
        ValueError: On missing columns or a non-finite comparison column.
    """
    missing = [column for column in _COMPARISON_COLUMNS if column not in comparison]
    if missing:
        msg = f"comparison is missing required columns: {missing}"
        raise ValueError(msg)
    observed = comparison["observed_total"].to_numpy(dtype="float64")
    predicted = comparison["point_estimate"].to_numpy(dtype="float64")
    lower = comparison["lower"].to_numpy(dtype="float64")
    upper = comparison["upper"].to_numpy(dtype="float64")
    for name, values in (
        ("observed_total", observed),
        ("point_estimate", predicted),
        ("lower", lower),
        ("upper", upper),
    ):
        if not np.all(np.isfinite(values)):
            msg = f"{name} must be finite (no NaN/inf)."
            raise ValueError(msg)

    observed_rank = quintile_ranks(comparison["observed_total"], n_groups=n_groups)
    predicted_rank = quintile_ranks(comparison["point_estimate"], n_groups=n_groups)
    confusion = transition_counts(observed_rank, predicted_rank, n_groups=n_groups)
    rank_difference = np.abs(observed_rank.to_numpy() - predicted_rank.to_numpy())
    within_band = (observed >= lower) & (observed <= upper)
    return {
        "n": len(comparison),
        "observed_distribution": wave_distribution_summary(observed),
        "imputed_distribution": wave_distribution_summary(predicted),
        "imputed_distribution_across_draws": _draw_level_distribution(
            comparison, imputed_draws, level=level
        ),
        "confusion_counts": confusion.tolist(),
        "exact_quintile_accuracy": float(
            (observed_rank.to_numpy() == predicted_rank.to_numpy()).mean()
        ),
        "mean_abs_quintile_error": float(rank_difference.mean()),
        "rank_correlation": rank_correlation(observed, predicted),
        "band_coverage": float(within_band.mean()),
        "median_abs_error": float(np.median(np.abs(observed - predicted))),
    }


def _draw_level_distribution(
    comparison: pd.DataFrame,
    imputed_draws: pd.DataFrame | None,
    *,
    level: float,
) -> dict | None:
    """Draw-level imputed distribution over the comparison households, or `None`.

    Restricts `imputed_draws` to the households scored in `comparison` (the
    completed-component truth set) so the draw-level statistics cover the same
    households as `observed_distribution`.
    """
    if imputed_draws is None:
        return None
    keys = comparison[_HH_KEYS].drop_duplicates()
    restricted = imputed_draws.merge(keys, on=_HH_KEYS, how="inner")
    return distribution_across_draws(restricted, level=level)
