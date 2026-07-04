"""Canonical wealth ontology and three-axis cell-provenance enums."""

from collections.abc import Mapping
from enum import Enum
from types import MappingProxyType


class CanonicalComponent(Enum):
    """Wealth components, normalized across waves by the registry."""

    OWNER_OCCUPIED_PROPERTY_GROSS = "owner_occupied_property_gross"
    OWNER_OCCUPIED_MORTGAGE = "owner_occupied_mortgage"
    OTHER_REAL_ESTATE = "other_real_estate"
    FINANCIAL_ASSETS = "financial_assets"
    PRIVATE_PENSION = "private_pension"
    BUSINESS_ASSETS = "business_assets"
    VEHICLES = "vehicles"
    CONSUMER_DEBT = "consumer_debt"


class RawInputState(Enum):
    """Axis A — the raw response state of a person-component cell."""

    OBSERVED_COMPLETE = "observed_complete"
    MISSING_SHARE = "missing_share"
    MISSING_AMOUNT = "missing_amount"
    MISSING_FILTER = "missing_filter"
    PUNR = "punr"
    STRUCTURAL_ABSENT = "structural_absent"
    INCONSISTENT = "inconsistent"
    AMBIGUOUS = "ambiguous"


class AlignmentEvidence(Enum):
    """Axis B — how a raw-derived value compares to the official completed value."""

    AGREES_ALL = "agrees_all"
    AGREES_MEAN = "agrees_mean"
    OFFICIAL_DIFFERS = "official_differs"
    FLAG_CONFLICT = "flag_conflict"
    UNAVAILABLE = "unavailable"


class HarnessAction(Enum):
    """Axis C — the action this harness took on a cell."""

    PRESERVED = "preserved"
    DETERMINISTIC_EDIT = "deterministic_edit"
    OWNERSHIP_IMPUTED = "ownership_imputed"
    AMOUNT_PMM = "amount_pmm"
    SHARE_IMPUTED = "share_imputed"
    PUNR_RESIDUAL = "punr_residual"
    STATISTICAL_REPLACEMENT = "statistical_replacement"
    SUPPRESSED = "suppressed"


LIABILITY_COMPONENTS: frozenset[CanonicalComponent] = frozenset(
    {
        CanonicalComponent.OWNER_OCCUPIED_MORTGAGE,
        CanonicalComponent.CONSUMER_DEBT,
    }
)

# Secured liability -> the asset that backs it. A secured liability exists only for
# owners of its backing asset and is drawn conditional on that asset's realised
# ownership, so no recipient can hold the liability without the asset. Consumer debt is
# unsecured and absent here. Drives the coupled property/mortgage draw in `simulate.py`.
SECURED_BY: Mapping[CanonicalComponent, CanonicalComponent] = MappingProxyType(
    {
        CanonicalComponent.OWNER_OCCUPIED_MORTGAGE: (
            CanonicalComponent.OWNER_OCCUPIED_PROPERTY_GROSS
        ),
    }
)


def component_sign(component: CanonicalComponent) -> int:
    """Return `-1` for liability components and `+1` for assets.

    Args:
        component: The wealth component (must be a `CanonicalComponent`).

    Returns:
        The sign used when summing components into net wealth.

    Raises:
        TypeError: If `component` is not a `CanonicalComponent` (e.g. a raw string),
            which would otherwise silently be classified as an asset.
    """
    if not isinstance(component, CanonicalComponent):
        msg = (
            "component must be a CanonicalComponent, got "
            f"{type(component).__name__}; convert via CanonicalComponent(value) first."
        )
        raise TypeError(msg)
    return -1 if component in LIABILITY_COMPONENTS else 1
