"""Behavior of the populated wealth-battery registry."""

import pytest

from soep_preparation.wealth_imputation.components import CanonicalComponent
from soep_preparation.wealth_imputation.registry import (
    AggregationRule,
    VerificationStatus,
    fail_if_unresolved_required,
)
from soep_preparation.wealth_imputation.registry_content import REGISTRY_ENTRIES

_FIRST_WAVE = 2002
_FIRST_SPLIT_WAVE = 2007
_PREDICTION_WAVE = 2022
_WAVES = (_FIRST_WAVE, _FIRST_SPLIT_WAVE, 2012, 2017, _PREDICTION_WAVE)
_SHARE_COMPONENTS = frozenset(
    {
        CanonicalComponent.OWNER_OCCUPIED_PROPERTY_GROSS,
        CanonicalComponent.OWNER_OCCUPIED_MORTGAGE,
        CanonicalComponent.OTHER_REAL_ESTATE,
        CanonicalComponent.FINANCIAL_ASSETS,
    }
)


def test_registry_has_one_entry_per_component_and_wave():
    """Every canonical component is declared for all five wealth-module waves."""
    assert len(REGISTRY_ENTRIES) == len(CanonicalComponent) * len(_WAVES)


def test_each_component_appears_in_every_wave():
    """The component-wave grid is complete with no duplicates."""
    grid = {(entry.component, entry.wave) for entry in REGISTRY_ENTRIES}
    expected = {
        (component, wave) for component in CanonicalComponent for wave in _WAVES
    }
    assert grid == expected


@pytest.mark.parametrize("component", sorted(CanonicalComponent, key=lambda c: c.value))
def test_property_components_use_the_same_variable_across_post_2002_waves(
    component: CanonicalComponent,
) -> None:
    """A component's raw variable is identical across 2007-2022 (wave-invariance)."""
    names = {
        entry.wave: entry.raw_variable
        for entry in REGISTRY_ENTRIES
        if entry.component is component and entry.wave != _FIRST_WAVE
    }
    assert len(set(names.values())) == 1


def test_private_pension_switches_from_combined_to_insurances_at_2007():
    """Private provision is `i0100a` in 2002 and `h0100a` from 2007 on."""
    names = {
        entry.wave: entry.raw_variable
        for entry in REGISTRY_ENTRIES
        if entry.component is CanonicalComponent.PRIVATE_PENSION
    }
    assert names[_FIRST_WAVE] == "i0100a"
    assert names[_FIRST_SPLIT_WAVE] == "h0100a"


def test_share_components_apply_the_ownership_share_rule():
    """Property and financial components carry a share and the share-then-sum rule."""
    for entry in REGISTRY_ENTRIES:
        if entry.component in _SHARE_COMPONENTS:
            assert entry.aggregation_rule is AggregationRule.PERSON_SHARE_THEN_PLAIN_SUM
            assert entry.ownership_share_variable is not None


def test_direct_components_have_no_ownership_share():
    """Pension, business, vehicles, and consumer debt are person-direct, no share."""
    for entry in REGISTRY_ENTRIES:
        if entry.component not in _SHARE_COMPONENTS:
            assert entry.aggregation_rule is AggregationRule.PERSON_DIRECT_PLAIN_SUM
            assert entry.ownership_share_variable is None


def test_2022_entries_are_unresolved_and_release_critical():
    """The unreleased 2022 module is UNRESOLVED and required for release."""
    for entry in REGISTRY_ENTRIES:
        if entry.wave == _PREDICTION_WAVE:
            assert entry.verification_status is VerificationStatus.UNRESOLVED
            assert entry.required_for_release


def test_pre_2022_entries_are_resolved():
    """Every pre-2022 entry is VERIFIED or INFERRED, never UNRESOLVED."""
    for entry in REGISTRY_ENTRIES:
        if entry.wave != _PREDICTION_WAVE:
            assert entry.verification_status is not VerificationStatus.UNRESOLVED


def test_vehicles_uses_the_v41_name_not_the_sp272_tangible_name():
    """Vehicles probes `v0100a` (V41), not the historical `t0100a`."""
    vehicles = [
        entry
        for entry in REGISTRY_ENTRIES
        if entry.component is CanonicalComponent.VEHICLES
    ]
    assert {entry.raw_variable for entry in vehicles} == {"v0100a"}


def test_release_gate_blocks_until_2022_is_resolved():
    """`fail_if_unresolved_required` raises while 2022 stays unresolved."""
    with pytest.raises(
        ValueError, match="Release-critical registry entries are unresolved"
    ):
        fail_if_unresolved_required(REGISTRY_ENTRIES)


def test_training_waves_alone_pass_the_release_gate():
    """The resolved pre-2022 subset clears the release gate."""
    training = [entry for entry in REGISTRY_ENTRIES if entry.wave != _PREDICTION_WAVE]
    fail_if_unresolved_required(training)  # no raise
