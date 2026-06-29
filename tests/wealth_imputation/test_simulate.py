"""Behavior of the joint-draw household-wealth simulation."""

import numpy as np
import pandas as pd
import pytest

from soep_preparation.wealth_imputation.components import (
    CanonicalComponent,
    component_sign,
)
from soep_preparation.wealth_imputation.simulate import (
    ComponentDrawConfig,
    ResidualDrawConfig,
    _draw_secured_housing,
    simulate_household_total_draws,
    simulate_household_totals,
)
from soep_preparation.wealth_imputation.value_generator import draw_component


def _property_config(ownership_prob: np.ndarray) -> ComponentDrawConfig:
    n = ownership_prob.shape[0]
    return ComponentDrawConfig(
        component=CanonicalComponent.OWNER_OCCUPIED_PROPERTY_GROSS,
        ownership_prob=ownership_prob,
        ownership_share=np.ones(n),
        recipient_predicted=np.full(n, 300.0),
        donor_predicted=np.array([300.0]),
        donor_observed=np.array([300.0]),
        scale=300.0,
        k=1,
    )


def _mortgage_config(ownership_prob: np.ndarray) -> ComponentDrawConfig:
    n = ownership_prob.shape[0]
    return ComponentDrawConfig(
        component=CanonicalComponent.OWNER_OCCUPIED_MORTGAGE,
        ownership_prob=ownership_prob,
        ownership_share=np.ones(n),
        recipient_predicted=np.full(n, 50.0),
        donor_predicted=np.array([50.0]),
        donor_observed=np.array([50.0]),
        scale=50.0,
        k=1,
    )


def test_draw_secured_housing_zeros_mortgage_for_non_property_owners():
    """A recipient drawn as a non-property-owner gets mortgage 0 (ownership and amount).

    The mortgage incidence probability is 1, so independent draws would always assign a
    mortgage; the property probability is 0, so no recipient owns property. Coupling
    forces the mortgage to zero -- ownership `False`, gross amount `0`, person value `0`
    -- in every draw.
    """
    property_draw, mortgage_draw = _draw_secured_housing(
        _property_config(np.array([0.0])),
        _mortgage_config(np.array([1.0])),
        rng=np.random.default_rng(seed=0),
    )
    assert not property_draw.owns.any()
    assert not mortgage_draw.owns.any()
    np.testing.assert_array_equal(mortgage_draw.gross_amount, [0.0])
    np.testing.assert_array_equal(mortgage_draw.person_value, [0.0])


def test_draw_secured_housing_allows_a_mortgage_for_a_property_owner():
    """A recipient drawn as a property owner may still receive its independent mortgage.

    With property and mortgage incidence both certain, the property owner keeps its
    coupled mortgage of 50, so coupling removes only incoherent mortgages, not coherent
    ones.
    """
    property_draw, mortgage_draw = _draw_secured_housing(
        _property_config(np.array([1.0])),
        _mortgage_config(np.array([1.0])),
        rng=np.random.default_rng(seed=0),
    )
    assert property_draw.owns.all()
    assert mortgage_draw.owns.all()
    np.testing.assert_allclose(mortgage_draw.gross_amount, [50.0], atol=1e-6)


def test_coupling_removes_the_negative_left_tail_from_lone_mortgages():
    """Coupling lifts the component-only negatives and p1 to the donor-supported range.

    Property ownership is rare (0.1) but mortgage ownership is common (0.9): independent
    draws manufacture households with a 50 mortgage and no property, a net total of -50
    that no donor exhibits, so a large negative share and a deeply negative p1. The
    coupled draw zeros those lone mortgages, so the only net totals are 0 (no property)
    or +250 (property net of its mortgage); the negative share collapses and p1 rises to
    the donor-supported 0.
    """
    recipients = pd.DataFrame({"p_id": [1], "hh_id": [10], "survey_year": [2017]})
    coupled = simulate_household_total_draws(
        recipients,
        [_property_config(np.array([0.1])), _mortgage_config(np.array([0.9]))],
        n_draws=2000,
        rng=np.random.default_rng(seed=1),
    )["household_total_draw"].to_numpy()
    independent = _independent_property_mortgage_draws(
        _property_config(np.array([0.1])),
        _mortgage_config(np.array([0.9])),
        n_draws=2000,
        seed=1,
    )
    coupled_negative_share = float(np.mean(coupled < 0.0))
    independent_negative_share = float(np.mean(independent < 0.0))
    assert coupled_negative_share == 0.0
    assert independent_negative_share > 0.5
    assert np.percentile(coupled, 1) == 0.0
    assert np.percentile(independent, 1) < 0.0


def _independent_property_mortgage_draws(
    property_config: ComponentDrawConfig,
    mortgage_config: ComponentDrawConfig,
    *,
    n_draws: int,
    seed: int,
) -> np.ndarray:
    """Draw property and mortgage independently (no coupling) as a regression baseline.

    Each draws its own component without the secured-pair zeroing, so a recipient can
    receive a mortgage with no property -- the artefact the coupling removes.
    """
    rng = np.random.default_rng(seed=seed)
    totals = np.empty(n_draws, dtype="float64")
    for draw in range(n_draws):
        net = 0.0
        for config in (property_config, mortgage_config):
            drawn = draw_component(
                ownership_prob=config.ownership_prob,
                ownership_share=config.ownership_share,
                recipient_predicted=config.recipient_predicted,
                donor_predicted=config.donor_predicted,
                donor_observed=config.donor_observed,
                scale=config.scale,
                k=config.k,
                rng=rng,
            )
            net += component_sign(config.component) * float(drawn.person_value[0])
        totals[draw] = net
    return totals


def test_simulate_never_assigns_a_mortgage_without_property_across_draws():
    """No household carries a mortgage without owner-occupied property in any draw.

    Property incidence is 0 and mortgage incidence is 1; the coupled draw must zero the
    mortgage every draw, so the net total stays 0 and never goes negative.
    """
    recipients = pd.DataFrame({"p_id": [1], "hh_id": [10], "survey_year": [2017]})
    draws = simulate_household_total_draws(
        recipients,
        [_property_config(np.array([0.0])), _mortgage_config(np.array([1.0]))],
        n_draws=200,
        rng=np.random.default_rng(seed=0),
    )
    np.testing.assert_array_equal(
        draws["household_total_draw"].to_numpy(), np.zeros(200)
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


def _residual_500() -> ResidualDrawConfig:
    return ResidualDrawConfig(
        recipient_predicted=np.array([500.0]),
        donor_predicted=np.array([500.0]),
        donor_observed=np.array([500.0]),
        k=1,
    )


def test_simulate_draws_keep_the_primary_total_component_only_with_a_residual():
    """With a residual config the primary total stays component-only."""
    draws = simulate_household_total_draws(
        _one_person_household(),
        [_financial_config()],
        n_draws=3,
        rng=np.random.default_rng(seed=0),
        residual_config=_residual_500(),
    )
    np.testing.assert_allclose(
        draws["household_total_draw"].to_numpy(), [100.0, 100.0, 100.0], atol=1e-6
    )


def test_simulate_draws_residual_inclusive_total_adds_the_drawn_residual():
    """The residual-inclusive column adds the sole donor's residual of 500 per draw."""
    draws = simulate_household_total_draws(
        _one_person_household(),
        [_financial_config()],
        n_draws=3,
        rng=np.random.default_rng(seed=0),
        residual_config=_residual_500(),
    )
    np.testing.assert_allclose(
        draws["residual_inclusive_total_draw"].to_numpy(),
        [600.0, 600.0, 600.0],
        atol=1e-6,
    )


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
