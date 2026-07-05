"""Tests for the multiply-imputed 2022 wealth replicate engine."""

from collections.abc import Mapping
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pandas as pd
import pytest

from soep_preparation.wealth_imputation import replicates as replicates_module
from soep_preparation.wealth_imputation.replicates import (
    ProjectionReplicates,
    apply_transport_scenario,
    apply_transport_shock,
    bayesian_bootstrap_weights,
    build_implicates_metadata,
    count_implicate_swaps,
    draw_transport_shocks,
    impute_replicates,
    layer_ablation_summary,
    official_wealth_aggregates,
    replicate_mc_summary,
    robust_total_scale,
    select_implicate_modules,
    select_released_implicates,
    transport_log_scale_excluding_largest_step,
    transport_log_scale_from_fold_errors,
    transport_scale_from_official_aggregates,
    transport_scenario_summary,
)

_RUN_IMPUTATION = "soep_preparation.wealth_imputation.impute.run_imputation"


def _stub_result() -> SimpleNamespace:
    """A fixed imputation result standing in for one `run_imputation` call."""
    intervals = pd.DataFrame(
        {"hh_id": [101, 102, 103], "point_estimate": [10_000.0, -5_000.0, 250_000.0]}
    )
    return SimpleNamespace(intervals=intervals)


def _run_engine_full(
    *, n_replicates: int, transport_log_scale: float, n_draws: int = 1
) -> ProjectionReplicates:
    with mock.patch(_RUN_IMPUTATION, return_value=_stub_result()):
        return impute_replicates(
            {},
            n_replicates=n_replicates,
            base_seed=0,
            transport_log_scale=transport_log_scale,
            total_scale=100_000.0,
            n_draws=n_draws,
            k=3,
        )


def _run_engine(
    *, n_replicates: int, transport_log_scale: float, n_draws: int = 1
) -> pd.DataFrame:
    return _run_engine_full(
        n_replicates=n_replicates,
        transport_log_scale=transport_log_scale,
        n_draws=n_draws,
    ).transport_scenario


def test_bayesian_bootstrap_weights_has_one_weight_per_unit() -> None:
    """The weight vector has exactly one entry per training unit."""
    weights = bayesian_bootstrap_weights(n_units=50, seed=0)
    assert weights.shape == (50,)


def test_bayesian_bootstrap_weights_sum_to_n_units() -> None:
    """Weights are scaled to average one, so they sum to the unit count."""
    weights = bayesian_bootstrap_weights(n_units=200, seed=0)
    np.testing.assert_allclose(weights.sum(), 200.0, rtol=1e-9)


def test_bayesian_bootstrap_weights_are_non_negative() -> None:
    """A frequency weight can be zero but never negative."""
    weights = bayesian_bootstrap_weights(n_units=200, seed=3)
    assert weights.min() >= 0.0


def test_bayesian_bootstrap_weights_are_reproducible_for_a_seed() -> None:
    """The same seed reproduces the identical weight vector."""
    first = bayesian_bootstrap_weights(n_units=100, seed=7)
    second = bayesian_bootstrap_weights(n_units=100, seed=7)
    np.testing.assert_array_equal(first, second)


def test_bayesian_bootstrap_weights_differ_across_seeds() -> None:
    """Different replicate seeds draw different weights (parameter uncertainty)."""
    first = bayesian_bootstrap_weights(n_units=100, seed=1)
    second = bayesian_bootstrap_weights(n_units=100, seed=2)
    assert not np.array_equal(first, second)


@pytest.mark.parametrize("bad_n", [0, -5])
def test_bayesian_bootstrap_weights_rejects_non_positive_unit_count(bad_n: int) -> None:
    """A replicate needs at least one unit to reweight."""
    with pytest.raises(ValueError, match="n_units"):
        bayesian_bootstrap_weights(n_units=bad_n, seed=0)


def test_transport_log_scale_is_rms_of_fold_errors() -> None:
    """The transport scale is the root-mean-square forward-prediction log error."""
    scale = transport_log_scale_from_fold_errors([0.1, -0.1, 0.2])
    np.testing.assert_allclose(scale, np.sqrt((0.01 + 0.01 + 0.04) / 3), rtol=1e-9)


def test_transport_log_scale_is_zero_for_perfect_folds() -> None:
    """No forward-prediction error implies no transport uncertainty."""
    assert transport_log_scale_from_fold_errors([0.0, 0.0]) == 0.0


def test_transport_log_scale_rejects_no_folds() -> None:
    """Calibration needs at least one rolling-origin fold error."""
    with pytest.raises(ValueError, match="fold"):
        transport_log_scale_from_fold_errors([])


def test_draw_transport_shocks_has_one_shock_per_replicate() -> None:
    """Each replicate receives exactly one systematic shock."""
    shocks = draw_transport_shocks(n_replicates=60, log_scale=0.3, seed=0)
    assert shocks.shape == (60,)


def test_draw_transport_shocks_are_zero_when_scale_is_zero() -> None:
    """A zero transport scale leaves every replicate unshifted."""
    shocks = draw_transport_shocks(n_replicates=20, log_scale=0.0, seed=0)
    np.testing.assert_array_equal(shocks, np.zeros(20))


def test_draw_transport_shocks_match_the_calibrated_scale() -> None:
    """Across many replicates the shock spread reproduces the calibrated log scale."""
    shocks = draw_transport_shocks(n_replicates=100_000, log_scale=0.25, seed=1)
    np.testing.assert_allclose(shocks.std(), 0.25, rtol=0.02)


def test_draw_transport_shocks_are_reproducible_for_a_seed() -> None:
    """The same seed reproduces the identical shock draws."""
    first = draw_transport_shocks(n_replicates=40, log_scale=0.2, seed=5)
    second = draw_transport_shocks(n_replicates=40, log_scale=0.2, seed=5)
    np.testing.assert_array_equal(first, second)


def test_draw_transport_shocks_reject_negative_scale() -> None:
    """A log scale is a standard deviation and cannot be negative."""
    with pytest.raises(ValueError, match="log_scale"):
        draw_transport_shocks(n_replicates=10, log_scale=-0.1, seed=0)


def test_transport_scale_from_official_aggregates_is_log_growth_dispersion() -> None:
    """The scale is the sample SD of the aggregate's consecutive log-growth steps."""
    aggregates = {2002: 100.0, 2007: 110.0, 2012: 132.0, 2017: 145.2}
    steps = np.diff(np.log([100.0, 110.0, 132.0, 145.2]))
    scale = transport_scale_from_official_aggregates(aggregates)
    np.testing.assert_allclose(scale, steps.std(ddof=1), rtol=1e-9)


def test_transport_scale_from_official_aggregates_is_zero_for_constant_growth() -> None:
    """A constant growth rate implies no transport dispersion."""
    aggregates = {2002: 100.0, 2007: 110.0, 2012: 121.0, 2017: 133.1}
    scale = transport_scale_from_official_aggregates(aggregates)
    np.testing.assert_allclose(scale, 0.0, atol=1e-9)


def test_transport_log_scale_excluding_largest_step_drops_the_extreme_step() -> None:
    """The bounded scale is the SD of the log-growth steps without the largest one."""
    aggregates = {2002: 100.0, 2007: 110.0, 2012: 132.0, 2017: 300.0}
    steps = np.diff(np.log([100.0, 110.0, 132.0, 300.0]))
    kept = np.delete(steps, int(np.argmax(np.abs(steps))))
    scale = transport_log_scale_excluding_largest_step(aggregates)
    np.testing.assert_allclose(scale, kept.std(ddof=1), rtol=1e-9)


def test_transport_log_scale_excluding_largest_step_is_below_the_full_scale() -> None:
    """Dropping the dominant step gives a strictly smaller, more conservative scale."""
    aggregates = {2002: 100.0, 2007: 110.0, 2012: 132.0, 2017: 300.0}
    bounded = transport_log_scale_excluding_largest_step(aggregates)
    full = transport_scale_from_official_aggregates(aggregates)
    assert bounded < full


def test_transport_log_scale_excluding_largest_step_rejects_too_few_waves() -> None:
    """Dropping a step and keeping a dispersion needs at least four waves."""
    with pytest.raises(ValueError, match="waves"):
        transport_log_scale_excluding_largest_step(
            {2007: 100.0, 2012: 110.0, 2017: 132.0}
        )


def test_transport_scale_from_official_aggregates_rejects_too_few_waves() -> None:
    """Estimating a dispersion needs at least two growth steps (three waves)."""
    with pytest.raises(ValueError, match="wave"):
        transport_scale_from_official_aggregates({2012: 100.0, 2017: 110.0})


def test_transport_scale_from_official_aggregates_rejects_non_positive_level() -> None:
    """A non-positive aggregate has no logarithm."""
    with pytest.raises(ValueError, match="positive"):
        transport_scale_from_official_aggregates({2007: 100.0, 2012: 0.0, 2017: 110.0})


def test_robust_total_scale_is_median_absolute_total() -> None:
    """The asinh knee is the median of the absolute net-wealth totals."""
    scale = robust_total_scale(np.array([-100.0, 200.0, 300.0, -400.0]))
    np.testing.assert_allclose(scale, 250.0, atol=1e-6)


def test_robust_total_scale_falls_back_to_one_without_positive_magnitude() -> None:
    """An all-zero total column still yields a usable positive scale."""
    assert robust_total_scale(np.array([0.0, 0.0])) == 1.0


def _official_hwealth() -> pd.DataFrame:
    """Household net-wealth totals across two wealth waves plus a non-wealth year."""
    return pd.DataFrame(
        {
            "survey_year": [2011, 2012, 2012, 2017, 2017],
            "hh_net_overall_wealth_a": [999.0, -100.0, 200.0, 300.0, -400.0],
        }
    )


def test_official_wealth_aggregates_sums_the_total_per_wave() -> None:
    """Each wealth wave's aggregate is the sum of its household totals."""
    result = official_wealth_aggregates(
        _official_hwealth(), total_column="hh_net_overall_wealth_a", waves=[2012, 2017]
    )
    assert result["wave_aggregates"] == {2012: 100.0, 2017: -100.0}


def test_official_wealth_aggregates_ignores_non_wealth_waves() -> None:
    """Years outside the wealth waves do not contribute an aggregate."""
    result = official_wealth_aggregates(
        _official_hwealth(), total_column="hh_net_overall_wealth_a", waves=[2012, 2017]
    )
    assert set(result["wave_aggregates"]) == {2012, 2017}


def test_official_wealth_aggregates_weights_the_wave_sum() -> None:
    """With a weight column each wave aggregate is the design-weighted total."""
    frame = pd.DataFrame(
        {
            "survey_year": [2012, 2012, 2017],
            "hh_net_overall_wealth_a": [100.0, 200.0, 300.0],
            "hh_weight": [2.0, 1.0, 1.0],
        }
    )
    result = official_wealth_aggregates(
        frame,
        total_column="hh_net_overall_wealth_a",
        waves=[2012, 2017],
        weight_column="hh_weight",
    )
    assert result["wave_aggregates"] == {2012: 400.0, 2017: 300.0}


def test_official_wealth_aggregates_weights_the_median_knee() -> None:
    """The knee is the weighted median absolute total when weights are supplied."""
    frame = pd.DataFrame(
        {
            "survey_year": [2012, 2012, 2012],
            "hh_net_overall_wealth_a": [100.0, 200.0, 300.0],
            "hh_weight": [3.0, 1.0, 1.0],
        }
    )
    result = official_wealth_aggregates(
        frame,
        total_column="hh_net_overall_wealth_a",
        waves=[2012],
        weight_column="hh_weight",
    )
    np.testing.assert_allclose(result["median_absolute_total"], 100.0, atol=1e-6)


def test_official_wealth_aggregates_drops_rows_with_missing_weight() -> None:
    """A household with a missing weight does not contribute to the aggregate."""
    frame = pd.DataFrame(
        {
            "survey_year": [2012, 2012],
            "hh_net_overall_wealth_a": [100.0, 200.0],
            "hh_weight": [1.0, np.nan],
        }
    )
    result = official_wealth_aggregates(
        frame,
        total_column="hh_net_overall_wealth_a",
        waves=[2012],
        weight_column="hh_weight",
    )
    assert result["wave_aggregates"] == {2012: 100.0}


@pytest.mark.parametrize("bad_weight", [-1.0, np.inf])
def test_official_wealth_aggregates_rejects_invalid_weights(bad_weight: float) -> None:
    """A negative or non-finite design weight corrupts the calibration and fails."""
    frame = pd.DataFrame(
        {
            "survey_year": [2012, 2012],
            "hh_net_overall_wealth_a": [100.0, 200.0],
            "hh_weight": [1.0, bad_weight],
        }
    )
    with pytest.raises(ValueError, match="weight"):
        official_wealth_aggregates(
            frame,
            total_column="hh_net_overall_wealth_a",
            waves=[2012],
            weight_column="hh_weight",
        )


def test_official_wealth_aggregates_rejects_zero_total_weight_for_a_wave() -> None:
    """A wave whose observed households all carry zero weight has no population."""
    frame = pd.DataFrame(
        {
            "survey_year": [2012, 2012],
            "hh_net_overall_wealth_a": [100.0, 200.0],
            "hh_weight": [0.0, 0.0],
        }
    )
    with pytest.raises(ValueError, match="weight"):
        official_wealth_aggregates(
            frame,
            total_column="hh_net_overall_wealth_a",
            waves=[2012],
            weight_column="hh_weight",
        )


def test_official_wealth_aggregates_are_unweighted_row_sums() -> None:
    """A raw row sum: duplicating a row doubles that wave's total (unweighted)."""
    base = pd.DataFrame(
        {"survey_year": [2012, 2017], "hh_net_overall_wealth_a": [100.0, 110.0]}
    )
    duplicated = pd.concat([base, base.iloc[[1]]], ignore_index=True)
    result = official_wealth_aggregates(
        duplicated, total_column="hh_net_overall_wealth_a", waves=[2012, 2017]
    )
    assert result["wave_aggregates"] == {2012: 100.0, 2017: 220.0}


def test_official_wealth_aggregates_reports_median_absolute_total() -> None:
    """The knee is the median absolute total across the wealth waves."""
    result = official_wealth_aggregates(
        _official_hwealth(), total_column="hh_net_overall_wealth_a", waves=[2012, 2017]
    )
    np.testing.assert_allclose(result["median_absolute_total"], 250.0, atol=1e-6)


def test_build_implicates_metadata_flags_no_observed_2022_wealth() -> None:
    """The guard records that 2022 wealth is projected, never observed."""
    meta = build_implicates_metadata(
        n_replicates=5, n_released=5, transport_log_scale=0.2, total_scale=100_000.0
    )
    assert meta["uses_observed_2022_wealth"] is False


def test_build_implicates_metadata_flags_transport_as_scenario_axis() -> None:
    """The transport shock is a separate labelled axis, not folded into the spread."""
    meta = build_implicates_metadata(
        n_replicates=5, n_released=5, transport_log_scale=0.2, total_scale=100_000.0
    )
    assert meta["transport_is_scenario_axis"] is True


def test_build_implicates_metadata_flags_weighted_pmm() -> None:
    """The PMM donor draw is weighted by the bootstrap weights."""
    meta = build_implicates_metadata(
        n_replicates=5, n_released=5, transport_log_scale=0.2, total_scale=100_000.0
    )
    assert meta["weighted_pmm"] is True


def test_build_implicates_metadata_flags_no_layer_decomposition() -> None:
    """Five replicates confound the uncertainty layers, so none is decomposable."""
    meta = build_implicates_metadata(
        n_replicates=5, n_released=5, transport_log_scale=0.2, total_scale=100_000.0
    )
    assert meta["layer_decomposition_available"] is False


def test_build_implicates_metadata_is_not_rubin_valid() -> None:
    """Without donor-implicate propagation the release is not fully Rubin-valid."""
    meta = build_implicates_metadata(
        n_replicates=5, n_released=5, transport_log_scale=0.2, total_scale=100_000.0
    )
    assert meta["rubin_valid"] is False


def test_build_implicates_metadata_flags_component_only() -> None:
    """The release omits the reconciliation residual, so it is component-only."""
    meta = build_implicates_metadata(
        n_replicates=5, n_released=5, transport_log_scale=0.2, total_scale=100_000.0
    )
    assert meta["component_only"] is True


def test_build_implicates_metadata_flags_donor_implicates_not_propagated() -> None:
    """Default: DIW donor implicates b-e are not threaded through the fits."""
    meta = build_implicates_metadata(
        n_replicates=5, n_released=5, transport_log_scale=0.2, total_scale=100_000.0
    )
    assert meta["donor_implicates_propagated"] is False


def test_build_implicates_metadata_records_donor_implicate_propagation() -> None:
    """When replicates draw distinct DIW implicates, the flag flips to true."""
    meta = build_implicates_metadata(
        n_replicates=5,
        n_released=5,
        transport_log_scale=0.2,
        total_scale=100_000.0,
        donor_implicates_propagated=True,
    )
    assert meta["donor_implicates_propagated"] is True


def test_build_implicates_metadata_flags_transport_not_mean_neutral() -> None:
    """The asinh-axis shock shifts euro-scale means, so it is not level-neutral."""
    meta = build_implicates_metadata(
        n_replicates=5, n_released=5, transport_log_scale=0.2, total_scale=100_000.0
    )
    assert meta["euro_scale_mean_neutral"] is False


def test_build_implicates_metadata_flags_transport_not_validated() -> None:
    """The transport scale is a calibrated prior, not a validated posterior."""
    meta = build_implicates_metadata(
        n_replicates=5, n_released=5, transport_log_scale=0.2, total_scale=100_000.0
    )
    assert meta["transport_posterior_validated"] is False


def test_build_implicates_metadata_records_the_transport_scale() -> None:
    """The calibrated transport log-scale is recorded for provenance."""
    meta = build_implicates_metadata(
        n_replicates=5, n_released=5, transport_log_scale=0.2, total_scale=100_000.0
    )
    np.testing.assert_allclose(meta["transport_log_scale"], 0.2, rtol=1e-9)


def test_impute_replicates_requires_one_draw_per_replicate() -> None:
    """An implicate is one predictive draw; `n_draws>1` would collapse to a median."""
    with pytest.raises(ValueError, match="n_draws"):
        _run_engine(n_replicates=5, transport_log_scale=0.0, n_draws=2)


def test_apply_transport_shock_is_not_euro_scale_mean_neutral() -> None:
    """Mean-zero asinh shocks bias a positive total's euro mean upward (`E[cosh]>1`)."""
    totals = np.array([250_000.0])
    up = apply_transport_shock(totals, 0.5, scale=100_000.0)[0]
    down = apply_transport_shock(totals, -0.5, scale=100_000.0)[0]
    assert (up + down) / 2 > 250_000.0


def test_apply_transport_shock_is_identity_at_zero_delta() -> None:
    """A zero shock leaves signed totals unchanged."""
    totals = np.array([-50_000.0, 0.0, 250_000.0])
    shocked = apply_transport_shock(totals, 0.0, scale=100_000.0)
    np.testing.assert_allclose(shocked, totals, atol=1e-6)


def test_apply_transport_shock_is_monotone_in_delta() -> None:
    """A larger shock maps a total to a larger value."""
    totals = np.array([250_000.0])
    low = apply_transport_shock(totals, 0.1, scale=100_000.0)[0]
    high = apply_transport_shock(totals, 0.5, scale=100_000.0)[0]
    assert high > low


def test_apply_transport_shock_shifts_a_positive_total_up_for_positive_delta() -> None:
    """A positive level shock makes a positive total larger."""
    shocked = apply_transport_shock(np.array([250_000.0]), 0.4, scale=100_000.0)
    assert shocked[0] > 250_000.0


def test_apply_transport_shock_shifts_a_negative_total_up_for_positive_delta() -> None:
    """A positive level shock moves an indebted total up toward zero, not down."""
    shocked = apply_transport_shock(np.array([-50_000.0]), 0.4, scale=100_000.0)
    assert shocked[0] > -50_000.0


def test_apply_transport_shock_rejects_non_positive_scale() -> None:
    """The asinh scale must be positive."""
    with pytest.raises(ValueError, match="scale"):
        apply_transport_shock(np.array([1.0]), 0.1, scale=0.0)


def test_impute_replicates_produces_one_column_per_replicate() -> None:
    """The engine emits one total column per requested replicate."""
    frame = _run_engine(n_replicates=5, transport_log_scale=0.3)
    draw_columns = [name for name in frame.columns if name.startswith("draw_")]
    assert draw_columns == [f"draw_{index}" for index in range(5)]


def test_impute_replicates_keeps_one_row_per_recipient_household() -> None:
    """Every recipient household appears once, keyed by `hh_id`."""
    frame = _run_engine(n_replicates=3, transport_log_scale=0.3)
    assert list(frame["hh_id"]) == [101, 102, 103]


def test_impute_replicates_transport_shock_makes_replicates_differ() -> None:
    """A positive transport scale gives each replicate a distinct systematic level."""
    frame = _run_engine(n_replicates=5, transport_log_scale=0.3)
    assert not np.allclose(frame["draw_0"].to_numpy(), frame["draw_1"].to_numpy())


def test_impute_replicates_are_identical_without_transport_under_a_fixed_fit() -> None:
    """With no transport shock and a fixed fit, replicates coincide."""
    frame = _run_engine(n_replicates=5, transport_log_scale=0.0)
    np.testing.assert_array_equal(
        frame["draw_0"].to_numpy(), frame["draw_4"].to_numpy()
    )


def test_impute_replicates_projection_is_free_of_the_transport_shock() -> None:
    """The projection frame carries no transport shock even with a nonzero scale."""
    result = _run_engine_full(n_replicates=5, transport_log_scale=0.3)
    np.testing.assert_array_equal(
        result.projection["draw_0"].to_numpy(),
        result.projection["draw_1"].to_numpy(),
    )


def test_impute_replicates_transport_scenario_differs_from_projection() -> None:
    """A positive transport scale shifts the scenario away from the projection."""
    result = _run_engine_full(n_replicates=5, transport_log_scale=0.3)
    assert not np.allclose(
        result.projection["draw_1"].to_numpy(),
        result.transport_scenario["draw_1"].to_numpy(),
    )


def test_transport_scenario_summary_isolates_a_positive_mean_shift() -> None:
    """Holding the base fixed, the convex shock lifts a net-positive aggregate mean."""
    result = _run_engine_full(n_replicates=5, transport_log_scale=0.3)
    summary = transport_scenario_summary(result.projection, result.transport_scenario)
    assert summary["aggregate_mean_shift"] > 0.0


def test_transport_scenario_summary_is_zero_shift_without_transport() -> None:
    """A zero transport scale leaves the aggregate mean unchanged."""
    result = _run_engine_full(n_replicates=5, transport_log_scale=0.0)
    summary = transport_scenario_summary(result.projection, result.transport_scenario)
    assert summary["aggregate_mean_shift"] == 0.0


def test_apply_transport_scenario_at_zero_scale_returns_the_projection() -> None:
    """A zero scale leaves every draw column unchanged."""
    result = _run_engine_full(n_replicates=3, transport_log_scale=0.0)
    scenario = apply_transport_scenario(
        result.projection, transport_log_scale=0.0, total_scale=100_000.0, base_seed=0
    )
    np.testing.assert_array_equal(
        scenario["draw_0"].to_numpy(), result.projection["draw_0"].to_numpy()
    )


def test_apply_transport_scenario_shocks_the_projection_at_a_positive_scale() -> None:
    """A positive scale shifts the draws away from the untouched projection."""
    result = _run_engine_full(n_replicates=3, transport_log_scale=0.0)
    scenario = apply_transport_scenario(
        result.projection, transport_log_scale=0.3, total_scale=100_000.0, base_seed=0
    )
    assert not np.allclose(
        scenario["draw_0"].to_numpy(), result.projection["draw_0"].to_numpy()
    )


def test_count_implicate_swaps_counts_columns_with_the_requested_sibling() -> None:
    """The realised count is how many `_a` columns carry the requested implicate."""
    modules = {
        "hwealth": pd.DataFrame(
            {"hh_financial_assets_value_a": [1.0], "hh_financial_assets_value_b": [2.0]}
        ),
        "pwealth": pd.DataFrame(
            {"financial_assets_value_a": [1.0], "financial_assets_value_b": [2.0]}
        ),
    }
    assert count_implicate_swaps(modules, "b") == 2


def test_count_implicate_swaps_is_zero_for_implicate_a() -> None:
    """Implicate `a` is the stored default, so nothing is swapped."""
    modules = {"hwealth": pd.DataFrame({"x_a": [1.0]}), "pwealth": pd.DataFrame()}
    assert count_implicate_swaps(modules, "a") == 0


def test_impute_replicates_reports_realised_donor_implicate_swaps() -> None:
    """The engine records the realised swap count per distinct implicate used."""
    result = _run_engine_full(n_replicates=2, transport_log_scale=0.0)
    assert result.donor_implicate_swaps == {"a": 0}


def test_layer_ablation_summary_reports_one_spread_per_configuration() -> None:
    """The ablation reports a between-replicate spread for each layer configuration."""
    with mock.patch(_RUN_IMPUTATION, return_value=_stub_result()):
        ablation = layer_ablation_summary({}, n_replicates=3, base_seed=0, k=3)
    assert set(ablation) == {
        "donor_draw_only",
        "plus_bootstrap",
        "plus_donor_implicate",
        "all_layers",
    }
    assert "aggregate_between_replicate_sd" in ablation["all_layers"]


def test_impute_replicates_varies_the_bootstrap_seed_per_replicate() -> None:
    """Each replicate refits under a distinct bootstrap seed."""
    spy = mock.MagicMock(return_value=_stub_result())
    with mock.patch(_RUN_IMPUTATION, spy):
        impute_replicates(
            {},
            n_replicates=3,
            base_seed=0,
            transport_log_scale=0.0,
            total_scale=100_000.0,
            n_draws=1,
            k=3,
        )
    seeds = {call.kwargs["bootstrap_seed"] for call in spy.call_args_list}
    assert len(seeds) == 3


def test_impute_replicates_rejects_non_positive_replicate_count() -> None:
    """At least one replicate is required."""
    with pytest.raises(ValueError, match="n_replicates"):
        _run_engine(n_replicates=0, transport_log_scale=0.3)


def test_select_implicate_modules_moves_the_implicate_into_the_a_slot() -> None:
    """Requesting implicate `b` overwrites each `_a` column with its `_b` sibling."""
    hwealth = pd.DataFrame(
        {"hh_financial_assets_value_a": [1.0], "hh_financial_assets_value_b": [2.0]}
    )
    out = select_implicate_modules({"hwealth": hwealth, "pwealth": pd.DataFrame()}, "b")
    assert out["hwealth"]["hh_financial_assets_value_a"].iloc[0] == 2.0


def test_select_implicate_modules_is_identity_for_implicate_a() -> None:
    """Implicate `a` is the stored default, so the modules are returned unchanged."""
    modules = {"hwealth": pd.DataFrame({"x_a": [1.0]}), "pwealth": pd.DataFrame()}
    assert select_implicate_modules(modules, "a") is modules


def test_select_implicate_modules_does_not_mutate_the_input() -> None:
    """Building a replicate's modules leaves the shared input frame untouched."""
    hwealth = pd.DataFrame({"c_a": [1.0], "c_b": [2.0]})
    select_implicate_modules({"hwealth": hwealth, "pwealth": pd.DataFrame()}, "b")
    assert hwealth["c_a"].iloc[0] == 1.0


def test_select_implicate_modules_rejects_a_missing_requested_suffix() -> None:
    """Requesting implicate `b` when a consumed component lacks its `_b` sibling fails.

    Otherwise the swap silently no-ops and the run reports donor implicates propagated
    while still imputing from implicate `a`.
    """
    modules = {
        "hwealth": pd.DataFrame({"hh_financial_assets_value_a": [1.0]}),
        "pwealth": pd.DataFrame({"financial_assets_value_a": [2.0]}),
    }
    with pytest.raises(ValueError, match="implicate"):
        select_implicate_modules(modules, "b")


def test_impute_replicates_cycles_donor_implicates_across_replicates() -> None:
    """Each replicate imputes from the next DIW donor implicate in turn."""
    seen: list[str] = []
    real = replicates_module.select_implicate_modules

    def spy(
        modules: Mapping[str, pd.DataFrame], implicate: str
    ) -> Mapping[str, pd.DataFrame]:
        seen.append(implicate)
        return real(modules, implicate)

    with (
        mock.patch(_RUN_IMPUTATION, return_value=_stub_result()),
        mock.patch.object(replicates_module, "select_implicate_modules", spy),
    ):
        impute_replicates(
            {},
            n_replicates=4,
            base_seed=0,
            transport_log_scale=0.0,
            total_scale=1.0,
            n_draws=1,
            k=3,
            donor_implicates=("a", "b"),
        )
    assert seen == ["a", "b", "a", "b"]


def _replicate_frame(n_replicates: int) -> pd.DataFrame:
    """A stand-in engine output: `hh_id` plus one total column per replicate."""
    frame = pd.DataFrame({"hh_id": [101, 102, 103]})
    for index in range(n_replicates):
        frame[f"draw_{index}"] = [10_000.0 * index, 20_000.0 + index, 300_000.0 - index]
    return frame


def test_select_released_implicates_yields_five_lettered_columns() -> None:
    """The released frame carries `hh_id` and five component-only projection draws."""
    released = select_released_implicates(_replicate_frame(10), n_released=5)
    assert list(released.columns) == [
        "hh_id",
        *[f"component_only_net_wealth_2022_{letter}" for letter in "abcde"],
    ]


def test_select_released_implicates_preserves_hh_id() -> None:
    """Every recipient household survives the selection, in order."""
    released = select_released_implicates(_replicate_frame(10), n_released=5)
    assert list(released["hh_id"]) == [101, 102, 103]


def test_select_released_implicates_is_the_replicates_when_count_matches() -> None:
    """With exactly five replicates, the implicates are those five columns verbatim."""
    frame = _replicate_frame(5)
    released = select_released_implicates(frame, n_released=5)
    np.testing.assert_array_equal(
        released["component_only_net_wealth_2022_a"].to_numpy(),
        frame["draw_0"].to_numpy(),
    )


def test_released_projection_prefix_excludes_transport_scenario_columns() -> None:
    """A projection-prefix selector returns only the five projection columns.

    The transport-scenario stem must not start with the projection stem, so the macro
    axis cannot be folded back into the released `a`-`e` projection set.
    """
    frame = _replicate_frame(5)
    released = select_released_implicates(
        frame, n_released=5, name="component_only_net_wealth_2022"
    ).merge(
        select_released_implicates(
            frame,
            n_released=5,
            name="transport_scenario_component_only_net_wealth_2022",
        ),
        on="hh_id",
        how="left",
    )
    selected = [
        column
        for column in released.columns
        if str(column).startswith("component_only_net_wealth_2022_")
    ]
    assert selected == [
        f"component_only_net_wealth_2022_{letter}" for letter in "abcde"
    ]


def test_select_released_implicates_rejects_too_few_replicates() -> None:
    """Releasing five implicates needs at least five replicates."""
    with pytest.raises(ValueError, match="n_released"):
        select_released_implicates(_replicate_frame(3), n_released=5)


def test_replicate_mc_summary_counts_the_replicates() -> None:
    """The summary records how many replicates the MC error is estimated from."""
    summary = replicate_mc_summary(_replicate_frame(8))
    assert summary["n_replicates"] == 8


def test_replicate_mc_summary_is_zero_spread_for_identical_replicates() -> None:
    """Identical replicates carry no Monte-Carlo spread in the aggregate."""
    frame = pd.DataFrame({"hh_id": [1, 2], "draw_0": [5.0, 7.0], "draw_1": [5.0, 7.0]})
    summary = replicate_mc_summary(frame)
    assert summary["aggregate_between_replicate_sd"] == 0.0


def test_replicate_mc_summary_reports_positive_spread_for_varying_replicates() -> None:
    """Replicates with different aggregates carry a positive replicate spread."""
    summary = replicate_mc_summary(_replicate_frame(6))
    assert summary["aggregate_between_replicate_sd"] > 0.0


def _four_replicate_means_100_to_400() -> pd.DataFrame:
    """Four replicates whose aggregate means are 100, 200, 300, 400."""
    return pd.DataFrame(
        {
            "hh_id": [1, 2],
            "draw_0": [100.0, 100.0],
            "draw_1": [200.0, 200.0],
            "draw_2": [300.0, 300.0],
            "draw_3": [400.0, 400.0],
        }
    )


def test_replicate_mc_summary_relative_between_replicate_sd_is_sd_over_mean() -> None:
    """`relative_between_replicate_sd` is the sample SD of replicate means over mean."""
    summary = replicate_mc_summary(_four_replicate_means_100_to_400())
    means = np.array([100.0, 200.0, 300.0, 400.0])
    sd = means.std(ddof=1)
    assert summary["aggregate_between_replicate_sd"] == sd
    assert summary["relative_between_replicate_sd"] == sd / means.mean()


def test_replicate_mc_summary_mc_se_of_the_mean_scales_with_sqrt_n() -> None:
    """The Monte-Carlo SE of the replicate mean is the between-replicate SD over √n."""
    summary = replicate_mc_summary(_four_replicate_means_100_to_400())
    sd = np.array([100.0, 200.0, 300.0, 400.0]).std(ddof=1)
    assert summary["mc_se_mean"] == sd / np.sqrt(4)


def test_replicate_mc_summary_does_not_call_the_spread_monte_carlo_error() -> None:
    """The between-replicate spread is the uncertainty, not simulation error."""
    summary = replicate_mc_summary(_four_replicate_means_100_to_400())
    assert "relative_mc_error" not in summary
