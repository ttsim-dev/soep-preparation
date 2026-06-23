"""Disclosure-safe wealth distribution and mobility summaries."""

import numpy as np
import pandas as pd
import pytest

from soep_preparation.wealth_imputation.wealth_dynamics import (
    build_dynamics_report,
    gini,
    quintile_ranks,
    top_share,
    transition_counts,
    transition_probabilities,
    wave_distribution_summary,
)


def test_gini_is_zero_for_perfect_equality():
    """An equal distribution has a Gini coefficient of zero."""
    assert gini(np.array([5.0, 5.0, 5.0, 5.0])) == pytest.approx(0.0)


def test_gini_for_one_holder_is_n_minus_one_over_n():
    """When one of four holds everything, the Gini is (n-1)/n = 0.75."""
    assert gini(np.array([0.0, 0.0, 0.0, 100.0])) == pytest.approx(0.75)


def test_gini_matches_the_hand_computed_value_for_a_small_ladder():
    """The Gini of 1,2,3,4 is 0.25 by the mean-absolute-difference definition."""
    assert gini(np.array([1.0, 2.0, 3.0, 4.0])) == pytest.approx(0.25)


def test_top_share_returns_fraction_held_by_the_top_group():
    """The top 10% of 1..10 is the single value 10, i.e. 10/55 of the total."""
    values = np.arange(1.0, 11.0)
    assert top_share(values, top_fraction=0.1) == pytest.approx(10.0 / 55.0)


def test_quintile_ranks_split_a_uniform_range_evenly():
    """Ten evenly spaced values fall two-per-quintile, ranked 1..5."""
    ranks = quintile_ranks(pd.Series(np.arange(1.0, 11.0)), n_groups=5)
    assert ranks.tolist() == [1, 1, 2, 2, 3, 3, 4, 4, 5, 5]


def test_wave_distribution_summary_reports_the_median():
    """The summary's 50th percentile is the median of the values."""
    summary = wave_distribution_summary(np.array([10.0, 20.0, 30.0, 40.0]))
    assert summary["quantiles"]["p50"] == pytest.approx(25.0)


def test_transition_counts_place_each_mover_in_its_from_to_cell():
    """A mover from quintile 1 to quintile 3 lands in row 1, column 3 (zero-based)."""
    counts = transition_counts(pd.Series([1, 2]), pd.Series([3, 2]), n_groups=5)
    assert counts[0, 2] == 1


def test_transition_probabilities_normalise_each_row_to_one():
    """Each origin-quintile row of the probability matrix sums to one."""
    counts = np.array([[3, 1], [0, 4]], dtype="float64")
    probabilities = transition_probabilities(counts)
    np.testing.assert_allclose(probabilities.sum(axis=1), [1.0, 1.0])


def test_build_dynamics_report_skips_waves_without_data():
    """A wave with no households is recorded and omitted, not raised on."""
    households = pd.DataFrame(
        {
            "hh_id": [1, 2, 3, 4],
            "survey_year": [2017, 2017, 2017, 2017],
            "net_wealth": [10.0, 20.0, 30.0, 40.0],
        }
    )
    roster = pd.DataFrame(
        {"p_id": [1, 2, 3, 4], "hh_id": [1, 2, 3, 4], "survey_year": [2017] * 4}
    )
    report = build_dynamics_report(
        households, roster, waves=(2012, 2017), n_groups=2, min_cell=1
    )
    assert report["metadata"]["waves_without_data"] == [2012]


def test_build_dynamics_report_covers_every_wave_and_horizon():
    """The report carries one distribution per wave and one transition per horizon."""
    waves = (2012, 2017)
    households = pd.DataFrame(
        {
            "hh_id": [1, 2, 3, 4, 1, 2, 3, 4],
            "survey_year": [2012] * 4 + [2017] * 4,
            "net_wealth": [10.0, 20.0, 30.0, 40.0, 15.0, 25.0, 35.0, 45.0],
        }
    )
    roster = pd.DataFrame(
        {
            "p_id": [1, 2, 3, 4, 1, 2, 3, 4],
            "hh_id": [1, 2, 3, 4, 1, 2, 3, 4],
            "survey_year": [2012] * 4 + [2017] * 4,
        }
    )
    report = build_dynamics_report(
        households, roster, waves=waves, n_groups=2, min_cell=1
    )
    assert set(report["distribution"]) == {"2012", "2017"}
    assert set(report["transitions"]) == {"2012->2017"}
