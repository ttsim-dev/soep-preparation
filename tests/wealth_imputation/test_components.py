"""Tests for canonical component ontology and provenance enums."""

from soep_preparation.wealth_imputation.components import (
    LIABILITY_COMPONENTS,
    CanonicalComponent,
    component_sign,
)


def test_component_sign_is_negative_for_liabilities_positive_for_assets() -> None:
    """Return -1 for liabilities and +1 for assets."""
    assert component_sign(CanonicalComponent.CONSUMER_DEBT) == -1
    assert component_sign(CanonicalComponent.OWNER_OCCUPIED_MORTGAGE) == -1
    assert component_sign(CanonicalComponent.FINANCIAL_ASSETS) == 1


def test_liability_components_are_exactly_mortgage_and_consumer_debt() -> None:
    """LIABILITY_COMPONENTS contains only mortgage and consumer debt."""
    assert (
        frozenset(
            {
                CanonicalComponent.OWNER_OCCUPIED_MORTGAGE,
                CanonicalComponent.CONSUMER_DEBT,
            }
        )
        == LIABILITY_COMPONENTS
    )
