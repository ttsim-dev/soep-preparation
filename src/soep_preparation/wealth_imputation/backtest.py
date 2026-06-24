"""Out-of-fold backtest: score imputed wealth against the observed wealth wave.

The proxy is validated by holding out the latest observed wealth wave (2017): fit on the
earlier waves, impute 2017 as if it were the target, and compare the imputed 2017
household totals with the observed 2017 totals. All outputs are disclosure-safe
aggregates -- distribution summaries, a quintile confusion matrix, rank accuracy, and
band coverage -- never a row-level value.

Only the modelled component sum is backtested. The residual to the official total cannot
be validated this way: the augmented official total (`n011h`) exists only from 2017, so
there is no earlier outcome wave to fit a residual on, and the comparison target is the
observed component sum, not the official total.
"""

import numpy as np
import pandas as pd

from soep_preparation.wealth_imputation.wealth_dynamics import (
    quintile_ranks,
    transition_counts,
    wave_distribution_summary,
)

_COMPARISON_COLUMNS = ("observed_total", "point_estimate", "lower", "upper")


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


def backtest_report(comparison: pd.DataFrame, *, n_groups: int = 5) -> dict:
    """Score imputed household totals against observed totals, disclosure-safe.

    Args:
        comparison: Columns `observed_total`, `point_estimate`, `lower`, `upper`; one
            row per household in the held-out wave.
        n_groups: Number of mobility groups for the rank comparison (5 for quintiles).

    Returns:
        A dict with the observed and imputed distribution summaries, the quintile
        confusion counts (rows = observed quintile, columns = predicted quintile), exact
        quintile accuracy, mean absolute quintile error, the rank correlation, the band
        coverage (fraction of observed values inside the imputed band), and the median
        absolute level error. No row-level value is included.

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
        "confusion_counts": confusion.tolist(),
        "exact_quintile_accuracy": float(
            (observed_rank.to_numpy() == predicted_rank.to_numpy()).mean()
        ),
        "mean_abs_quintile_error": float(rank_difference.mean()),
        "rank_correlation": rank_correlation(observed, predicted),
        "band_coverage": float(within_band.mean()),
        "median_abs_error": float(np.median(np.abs(observed - predicted))),
    }
