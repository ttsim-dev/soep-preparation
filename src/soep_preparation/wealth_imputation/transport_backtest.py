"""Leave-one-wave-out temporal-transport backtest.

The single-wave 2017 backtest gives one transport data point (train on the earlier
waves, predict 2017). It cannot say whether that fidelity is stable or a one-wave
accident. This backtest holds out *each* observed wealth wave in turn, fits the
component models on the other waves, imputes the held-out wave, and scores it -- so
transport quality is read across several target years, not at one.

Two truths are scored per held-out wave:

- the **completed-component sum** (apples-to-apples with the component-only total; the
  same truth the single-wave backtest uses); and
- the official all-wave net total **`w011h`** (`hh_net_overall_wealth_a`), which exists
  in every wealth wave. Its *level* is not comparable to the component-only total -- it
  excludes vehicles and carries the unmodelled business / other-real-estate residual --
  so it is scored on **rank only** (Spearman correlation and quintile agreement). Rank
  is exactly what a covariate / ordinal use of the proxy relies on, and `w011h` is the
  only official total available before 2017, which is why it anchors the cross-wave
  comparison.

All outputs are disclosure-safe aggregates. No row-level value is returned.
"""

from collections.abc import Mapping, Sequence

import numpy as np
import pandas as pd

from soep_preparation.wealth_imputation.backtest import (
    backtest_report,
    rank_correlation,
)
from soep_preparation.wealth_imputation.impute import (
    observed_component_total,
    run_imputation,
)
from soep_preparation.wealth_imputation.wealth_dynamics import quintile_ranks

_HH_KEYS = ["hh_id", "survey_year"]
_OFFICIAL_W011H_COLUMN = "hh_net_overall_wealth_a"
_STABILITY_METRICS = (
    "rank_correlation",
    "exact_quintile_accuracy",
    "band_coverage",
    "median_abs_error",
)


def run_transport_backtest(  # noqa: PLR0913 -- keyword-only run settings + wave sets
    modules: Mapping[str, pd.DataFrame],
    *,
    holdout_waves: Sequence[int],
    all_waves: Sequence[int],
    n_draws: int,
    seed: int,
    k: int,
    level: float = 0.9,
    n_groups: int = 5,
) -> dict:
    """Score the imputation on each held-out wave, fitting on the other waves.

    For every wave in `holdout_waves` the component models are fit on the remaining
    `all_waves`, the held-out wave is imputed out of fold, and the result is scored
    against both the completed-component truth and the official `w011h` total (rank
    only). A transport-stability summary then reports how each headline metric varies
    across the held-out waves -- a small spread is evidence the method transports.

    Args:
        modules: Cleaned `MODULES` frames (`hwealth`, `pwealth`, and the feature
            modules `run_imputation` needs).
        holdout_waves: The wealth waves to hold out and score, one at a time.
        all_waves: The full set of wealth waves; each held-out wave trains on the rest.
        n_draws: Donor draws per imputation.
        seed: Seed for the draw RNG.
        k: Nearest-donor pool size for PMM.
        level: Central level for the donor-spread bands and draw-level statistics.
        n_groups: Number of rank groups (5 for quintiles).

    Returns:
        A disclosure-safe dict with `per_holdout_wave` (one scorecard per held-out
        wave, each carrying the completed-component metrics plus a `vs_official_w011h`
        rank block) and `transport_stability` (the spread of each metric across waves).
    """
    per_wave = {}
    for wave in holdout_waves:
        training = tuple(other for other in all_waves if other != wave)
        result = run_imputation(
            modules,
            n_draws=n_draws,
            seed=seed,
            k=k,
            level=level,
            prediction_wave=wave,
            training_waves=training,
            keep_draws=True,
        )
        observed = observed_component_total(modules, wave)
        comparison = result.intervals.merge(
            observed, on=_HH_KEYS, how="inner", validate="one_to_one"
        )
        report = backtest_report(
            comparison,
            imputed_draws=result.component_only_draws,
            level=level,
            n_groups=n_groups,
        )
        report["vs_official_w011h"] = official_rank_comparison(
            result.intervals, official_total_truth(modules, wave), n_groups=n_groups
        )
        report["holdout_wave"] = wave
        report["training_waves"] = list(training)
        report["n_recipients"] = int(result.summary["n_recipients"])
        per_wave[str(wave)] = report
    return {
        "method": "leave_one_wave_out",
        "holdout_waves": list(holdout_waves),
        "all_waves": list(all_waves),
        "n_draws": n_draws,
        "seed": seed,
        "k": k,
        "level": level,
        "per_holdout_wave": per_wave,
        "transport_stability": _transport_stability(per_wave),
    }


def official_total_truth(
    modules: Mapping[str, pd.DataFrame], wave: int
) -> pd.DataFrame:
    """Return the observed official `w011h` net total per household in `wave`.

    `w011h` (`hh_net_overall_wealth_a`) is the net overall wealth populated in every
    wealth wave, so it can anchor a cross-wave rank comparison where the augmented
    `n011h` total (2017-only) cannot. Households without a finite total are dropped.

    Args:
        modules: Cleaned `MODULES` frames; uses `hwealth`.
        wave: The survey year whose official totals to return.

    Returns:
        Columns `hh_id`, `survey_year`, `official_total` (float64); one row per
        household in `wave` with a finite official total.
    """
    hwealth = modules["hwealth"]
    in_wave = hwealth.loc[hwealth["survey_year"] == wave]
    official = pd.to_numeric(in_wave[_OFFICIAL_W011H_COLUMN], errors="coerce")
    finite = official.notna().to_numpy()
    out = in_wave.loc[finite, _HH_KEYS].reset_index(drop=True)
    out["official_total"] = official[finite].to_numpy(dtype="float64")
    return out


def official_rank_comparison(
    intervals: pd.DataFrame, official_truth: pd.DataFrame, *, n_groups: int = 5
) -> dict:
    """Compare the imputed point estimate to the official total on rank only.

    The component-only point estimate and the official `w011h` total are on different
    levels, so only their ordering is comparable. This returns the Spearman rank
    correlation and quintile agreement over the households present in both frames.

    Args:
        intervals: Imputed intervals with `hh_id`, `survey_year`, `point_estimate`.
        official_truth: Output of `official_total_truth` for the same wave.
        n_groups: Number of rank groups (5 for quintiles).

    Returns:
        A dict with `n`, `rank_correlation`, `exact_quintile_accuracy`, and
        `mean_abs_quintile_error`.
    """
    merged = intervals.merge(
        official_truth, on=_HH_KEYS, how="inner", validate="one_to_one"
    )
    observed = merged["official_total"].to_numpy(dtype="float64")
    predicted = merged["point_estimate"].to_numpy(dtype="float64")
    observed_rank = quintile_ranks(merged["official_total"], n_groups=n_groups)
    predicted_rank = quintile_ranks(merged["point_estimate"], n_groups=n_groups)
    rank_difference = np.abs(observed_rank.to_numpy() - predicted_rank.to_numpy())
    return {
        "n": len(merged),
        "rank_correlation": rank_correlation(observed, predicted),
        "exact_quintile_accuracy": float(
            (observed_rank.to_numpy() == predicted_rank.to_numpy()).mean()
        ),
        "mean_abs_quintile_error": float(rank_difference.mean()),
    }


def _transport_stability(per_wave: Mapping[str, dict]) -> dict:
    """Summarise how each headline metric varies across the held-out waves.

    A small spread (max minus min) across waves is evidence the method transports
    temporally; a large spread says the single-wave result was wave-specific.
    """
    reports = list(per_wave.values())
    stability: dict = {"n_waves": len(reports)}
    for metric in _STABILITY_METRICS:
        stability[metric] = _spread([float(report[metric]) for report in reports])
    for metric in ("rank_correlation", "exact_quintile_accuracy"):
        stability[f"vs_official_w011h_{metric}"] = _spread(
            [float(report["vs_official_w011h"][metric]) for report in reports]
        )
    stability["negative_share_calibrated_count"] = sum(
        report.get("negative_share_calibrated") is True for report in reports
    )
    return stability


def _spread(values: list[float]) -> dict:
    """Return the min, max, mean, and max-minus-min spread of `values`."""
    return {
        "min": min(values),
        "max": max(values),
        "mean": float(np.mean(values)),
        "spread": max(values) - min(values),
    }
