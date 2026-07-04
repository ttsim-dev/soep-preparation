"""Behavior of the populated wealth-battery registry."""

import pytest

from soep_preparation.wealth_imputation.components import CanonicalComponent
from soep_preparation.wealth_imputation.registry import (
    AggregationRule,
    VerificationStatus,
    fail_if_unresolved_required,
)
from soep_preparation.wealth_imputation.registry_content import REGISTRY_ENTRIES

_PREDICTION_WAVE = 2022
_WAVES = (2002, 2007, 2012, 2017, _PREDICTION_WAVE)

# Components available directly in the reduced SOEP-Core V41 pwealth file.
_AVAILABLE_COMPONENTS = frozenset(
    {
        CanonicalComponent.OWNER_OCCUPIED_PROPERTY_GROSS,
        CanonicalComponent.FINANCIAL_ASSETS,
        CanonicalComponent.PRIVATE_PENSION,
        CanonicalComponent.VEHICLES,
        CanonicalComponent.CONSUMER_DEBT,
    }
)
# Components not separately sourceable from the SOEP-Core file.
_UNAVAILABLE_COMPONENTS = frozenset(
    {
        CanonicalComponent.OWNER_OCCUPIED_MORTGAGE,
        CanonicalComponent.OTHER_REAL_ESTATE,
        CanonicalComponent.BUSINESS_ASSETS,
    }
)
_SHARE_COMPONENTS = frozenset(
    {
        CanonicalComponent.OWNER_OCCUPIED_PROPERTY_GROSS,
        CanonicalComponent.OWNER_OCCUPIED_MORTGAGE,
        CanonicalComponent.OTHER_REAL_ESTATE,
        CanonicalComponent.FINANCIAL_ASSETS,
    }
)
_EXPECTED_NAMES = {
    CanonicalComponent.OWNER_OCCUPIED_PROPERTY_GROSS: "p0100a",
    CanonicalComponent.FINANCIAL_ASSETS: "f0100a",
    CanonicalComponent.PRIVATE_PENSION: "h0100a",
    CanonicalComponent.VEHICLES: "v0100a",
    CanonicalComponent.CONSUMER_DEBT: "c0100a",
}


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
def test_each_component_uses_the_same_variable_in_every_wave(
    component: CanonicalComponent,
) -> None:
    """A component's raw variable is identical across all waves (wave-invariance)."""
    names = {
        entry.raw_variable for entry in REGISTRY_ENTRIES if entry.component is component
    }
    assert len(names) == 1


@pytest.mark.parametrize(
    ("component", "expected"),
    sorted(_EXPECTED_NAMES.items(), key=lambda item: item[0].value),
)
def test_available_components_map_to_their_v41_column(
    component: CanonicalComponent,
    expected: str,
) -> None:
    """Each available component probes its confirmed SOEP-Core V41 column."""
    names = {
        entry.raw_variable for entry in REGISTRY_ENTRIES if entry.component is component
    }
    assert names == {expected}


def test_private_pension_uses_insurances_column_in_every_wave():
    """Private provision maps to `h0100a` in all waves, including 2002."""
    names = {
        entry.raw_variable
        for entry in REGISTRY_ENTRIES
        if entry.component is CanonicalComponent.PRIVATE_PENSION
    }
    assert names == {"h0100a"}


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


def test_available_pre_2022_entries_are_verified_and_required():
    """Present components are VERIFIED before 2022 and drive the release."""
    for entry in REGISTRY_ENTRIES:
        if entry.component in _AVAILABLE_COMPONENTS and entry.wave != _PREDICTION_WAVE:
            assert entry.verification_status is VerificationStatus.VERIFIED
            assert entry.required_for_release


def test_available_2022_entries_are_unresolved_and_required():
    """The unreleased 2022 values are UNRESOLVED but still release-critical."""
    for entry in REGISTRY_ENTRIES:
        if entry.component in _AVAILABLE_COMPONENTS and entry.wave == _PREDICTION_WAVE:
            assert entry.verification_status is VerificationStatus.UNRESOLVED
            assert entry.required_for_release


def test_unavailable_components_are_unresolved_and_not_required():
    """Components absent from SOEP-Core are UNRESOLVED and not release-critical."""
    for entry in REGISTRY_ENTRIES:
        if entry.component in _UNAVAILABLE_COMPONENTS:
            assert entry.verification_status is VerificationStatus.UNRESOLVED
            assert not entry.required_for_release


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
