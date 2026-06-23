"""Behavior of the joint-draw household-wealth simulation."""

import numpy as np
import pandas as pd
import pytest

from soep_preparation.wealth_imputation.components import CanonicalComponent
from soep_preparation.wealth_imputation.simulate import (
    ComponentDrawConfig,
    ResidualDrawConfig,
    simulate_household_totals,
)


def _one_person_household() -> pd.DataFrame:
    return pd.DataFrame({"p_id": [1], "hh_id": [10], "survey_year": [2017]})


def _financial_config() -> ComponentDrawConfig:
    """A single owner whose only donor holds exactly 100."""
    return ComponentDrawConfig(
        component=CanonicalComponent.FINANCIAL_ASSETS,
        ownership_prob=np.array([1.0]),
        ownership_share=np.array([1.0]),
        recipient_predicted=np.array([100.0]),
        donor_predicted=np.array([100.0]),
        donor_observed=np.array([100.0]),
        scale=100.0,
        k=1,
    )


def test_simulate_household_totals_point_estimate_equals_sole_donor_value():
    """With one certain owner and one donor, every draw yields the donor value."""
    result = simulate_household_totals(
        _one_person_household(),
        [_financial_config()],
        n_draws=3,
        rng=np.random.default_rng(seed=0),
    )
    np.testing.assert_allclose(result["point_estimate"].to_numpy(), [100.0], atol=1e-6)


def test_simulate_household_totals_adds_the_drawn_residual():
    """A residual config whose sole donor holds 500 shifts every total by 500."""
    residual = ResidualDrawConfig(
        recipient_predicted=np.array([500.0]),
        donor_predicted=np.array([500.0]),
        donor_observed=np.array([500.0]),
        k=1,
    )
    result = simulate_household_totals(
        _one_person_household(),
        [_financial_config()],
        n_draws=3,
        rng=np.random.default_rng(seed=0),
        residual_config=residual,
    )
    np.testing.assert_allclose(result["point_estimate"].to_numpy(), [600.0], atol=1e-6)


def _two_person_household() -> pd.DataFrame:
    return pd.DataFrame(
        {"p_id": [1, 2], "hh_id": [10, 10], "survey_year": [2017, 2017]}
    )


def test_simulate_household_totals_nets_assets_minus_liabilities_over_members():
    """Net total = members' financial assets minus the household's consumer debt."""
    financial = ComponentDrawConfig(
        component=CanonicalComponent.FINANCIAL_ASSETS,
        ownership_prob=np.array([1.0, 1.0]),
        ownership_share=np.array([1.0, 1.0]),
        recipient_predicted=np.array([100.0, 50.0]),
        donor_predicted=np.array([100.0, 50.0]),
        donor_observed=np.array([100.0, 50.0]),
        scale=100.0,
        k=1,
    )
    debt = ComponentDrawConfig(
        component=CanonicalComponent.CONSUMER_DEBT,
        ownership_prob=np.array([1.0, 0.0]),  # only person 1 holds debt
        ownership_share=np.array([1.0, 1.0]),
        recipient_predicted=np.array([20.0, 20.0]),
        donor_predicted=np.array([20.0]),
        donor_observed=np.array([20.0]),
        scale=100.0,
        k=1,
    )
    result = simulate_household_totals(
        _two_person_household(),
        [financial, debt],
        n_draws=2,
        rng=np.random.default_rng(seed=0),
    )
    # (100 + 50) financial - 20 consumer debt = 130.
    np.testing.assert_allclose(result["point_estimate"].to_numpy(), [130.0], atol=1e-6)


def test_simulate_household_totals_is_deterministic_for_a_fixed_seed():
    """The same seed reproduces the interval."""
    first = simulate_household_totals(
        _one_person_household(),
        [_financial_config()],
        n_draws=4,
        rng=np.random.default_rng(seed=5),
    )
    second = simulate_household_totals(
        _one_person_household(),
        [_financial_config()],
        n_draws=4,
        rng=np.random.default_rng(seed=5),
    )
    np.testing.assert_array_equal(
        first["point_estimate"].to_numpy(), second["point_estimate"].to_numpy()
    )


def test_simulate_household_totals_rejects_config_misaligned_with_recipients():
    """A config whose arrays do not match the recipient count fails closed."""
    bad = ComponentDrawConfig(
        component=CanonicalComponent.FINANCIAL_ASSETS,
        ownership_prob=np.array([1.0, 1.0]),  # two entries for one recipient
        ownership_share=np.array([1.0, 1.0]),
        recipient_predicted=np.array([100.0, 100.0]),
        donor_predicted=np.array([100.0]),
        donor_observed=np.array([100.0]),
        scale=100.0,
        k=1,
    )
    with pytest.raises(ValueError, match="recipients"):
        simulate_household_totals(
            _one_person_household(),
            [bad],
            n_draws=1,
            rng=np.random.default_rng(seed=0),
        )
