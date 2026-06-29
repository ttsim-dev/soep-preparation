"""Behavior of the 2017 cross-fitted residual validation."""

import numpy as np
import pytest

from soep_preparation.wealth_imputation.residual_backtest import (
    assign_folds,
    cross_fit_residual_draws,
    out_of_fold_donor_indices,
    score_residual_cross_fit,
)


def _monotone_fixture(
    n_rows: int = 200,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """A residual that is a monotone (affine) function of a single covariate.

    The held-out residual is then near-perfectly predictable from the donor pool, so a
    correct cross-fit yields a high rank correlation and high sign accuracy.
    """
    covariate = np.linspace(-3.0, 3.0, n_rows)
    observed_residual = 10_000.0 * covariate
    component_total = np.full(n_rows, 50_000.0)
    return covariate.reshape(-1, 1), observed_residual, component_total


def test_assign_folds_partitions_every_row_into_one_of_k_folds():
    """Every row gets exactly one fold label in `[0, n_folds)`."""
    folds = assign_folds(n_rows=23, n_folds=5, rng=np.random.default_rng(seed=0))
    assert set(np.unique(folds)).issubset(set(range(5)))


def test_assign_folds_balances_fold_sizes_within_one():
    """Fold sizes differ by at most one row (a balanced K-fold split)."""
    folds = assign_folds(n_rows=23, n_folds=5, rng=np.random.default_rng(seed=0))
    counts = np.bincount(folds, minlength=5)
    assert counts.max() - counts.min() <= 1


def test_out_of_fold_donor_indices_excludes_the_held_out_fold():
    """A household's own residual never enters its donor pool (strict out-of-fold)."""
    folds = np.array([0, 1, 0, 2, 1, 2])
    donors = out_of_fold_donor_indices(folds, held_out_fold=1)
    held_out = np.flatnonzero(folds == 1)
    assert not set(held_out.tolist()) & set(donors.tolist())


def test_out_of_fold_donor_indices_returns_all_other_rows():
    """The donor pool for a fold is exactly the rows in the other folds."""
    folds = np.array([0, 1, 0, 2, 1, 2])
    donors = out_of_fold_donor_indices(folds, held_out_fold=1)
    np.testing.assert_array_equal(donors, np.array([0, 2, 3, 5]))


def test_cross_fit_predicts_every_row_exactly_once():
    """Each held-out row receives a full set of draws (no row left unscored)."""
    design, observed_residual, _ = _monotone_fixture()
    result = cross_fit_residual_draws(
        design,
        observed_residual,
        n_folds=5,
        k=10,
        n_draws=50,
        rng=np.random.default_rng(seed=0),
    )
    assert result.draws.shape == (observed_residual.size, 50)


def test_cross_fit_donor_value_is_never_the_rows_own_observed_residual():
    """No drawn residual equals the held-out row's own observed residual."""
    design, observed_residual, _ = _monotone_fixture()
    result = cross_fit_residual_draws(
        design,
        observed_residual,
        n_folds=5,
        k=1,
        n_draws=20,
        rng=np.random.default_rng(seed=0),
    )
    own = observed_residual.reshape(-1, 1)
    assert not np.any(np.isclose(result.draws, own, atol=1e-9))


def test_score_rank_correlation_is_high_for_a_monotone_residual():
    """A residual monotone in a covariate is predicted with high rank correlation."""
    design, observed_residual, component_total = _monotone_fixture()
    result = cross_fit_residual_draws(
        design,
        observed_residual,
        n_folds=5,
        k=5,
        n_draws=50,
        rng=np.random.default_rng(seed=0),
    )
    report = score_residual_cross_fit(
        observed_residual, result.draws, component_total, level=0.9
    )
    assert report["rank_correlation"] > 0.9


def test_score_sign_accuracy_is_high_for_a_monotone_residual():
    """The predicted residual's sign matches the observed sign for most households."""
    design, observed_residual, component_total = _monotone_fixture()
    result = cross_fit_residual_draws(
        design,
        observed_residual,
        n_folds=5,
        k=5,
        n_draws=50,
        rng=np.random.default_rng(seed=0),
    )
    report = score_residual_cross_fit(
        observed_residual, result.draws, component_total, level=0.9
    )
    assert report["sign_accuracy"] > 0.9


def test_score_reports_disclosure_safe_aggregates_only():
    """The scorecard holds only counts/quantiles/correlations, no per-row arrays."""
    design, observed_residual, component_total = _monotone_fixture()
    result = cross_fit_residual_draws(
        design,
        observed_residual,
        n_folds=5,
        k=5,
        n_draws=50,
        rng=np.random.default_rng(seed=0),
    )
    report = score_residual_cross_fit(
        observed_residual, result.draws, component_total, level=0.9
    )
    assert all(
        np.ndim(value) == 0 or isinstance(value, dict) for value in report.values()
    )


def test_score_band_coverage_is_a_fraction():
    """Draw-band coverage of the observed residual is a share in `[0, 1]`."""
    design, observed_residual, component_total = _monotone_fixture()
    result = cross_fit_residual_draws(
        design,
        observed_residual,
        n_folds=5,
        k=5,
        n_draws=50,
        rng=np.random.default_rng(seed=0),
    )
    report = score_residual_cross_fit(
        observed_residual, result.draws, component_total, level=0.9
    )
    assert 0.0 <= report["band_coverage"] <= 1.0


def test_score_median_abs_error_is_zero_when_prediction_equals_observation():
    """A draw matrix equal to the observed residual has zero median absolute error."""
    observed_residual = np.array([-3.0, -1.0, 2.0, 4.0])
    component_total = np.zeros(4)
    draws = np.repeat(observed_residual.reshape(-1, 1), 10, axis=1)
    report = score_residual_cross_fit(
        observed_residual, draws, component_total, level=0.9
    )
    assert report["median_abs_error"] == pytest.approx(0.0)


def _sign_cancelling_draws() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """A draw matrix whose per-row median erases a 0.4 draw-level negative share.

    Every row carries the same 200 draws -- 80 of -100 and 120 of +100 -- so each row's
    median is +100 (no negative mass after collapsing to the median), while 40% of the
    draws are negative. Component totals are zero, so the residual is the whole total.
    """
    n_rows = 100
    pattern = np.array([-100.0] * 80 + [100.0] * 120)
    predicted_draws = np.tile(pattern, (n_rows, 1))
    component_total = np.zeros(n_rows)
    observed_residual = np.full(n_rows, 100.0)
    return observed_residual, predicted_draws, component_total


def test_score_renames_drawn_residual_total_to_median_point():
    """The median-collapsed total is reported under an explicitly median-point name."""
    observed_residual, predicted_draws, component_total = _sign_cancelling_draws()
    report = score_residual_cross_fit(
        observed_residual, predicted_draws, component_total, level=0.9
    )
    assert "total_with_drawn_residual" not in report
    assert "total_with_median_predicted_residual" in report


def test_score_draw_level_summary_preserves_the_negative_share_the_median_erases():
    """The across-draws summary keeps the 0.4 negative share the median collapses to 0.

    The median-point total has no negative mass (its p10 is +100), but the draw-level
    summary computes the negative share within each draw, so it reports the 0.4 the
    sign-cancelling draws actually carry.
    """
    observed_residual, predicted_draws, component_total = _sign_cancelling_draws()
    report = score_residual_cross_fit(
        observed_residual, predicted_draws, component_total, level=0.9
    )
    across = report["total_with_drawn_residual_across_draws"]
    assert across["negative_share"]["mean"] == pytest.approx(0.4)
