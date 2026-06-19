"""Canonical wealth ontology and three-axis cell-provenance enums."""

from enum import Enum


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


def component_sign(component: CanonicalComponent) -> int:
    """Return `-1` for liability components and `+1` for assets.

    Args:
        component: The wealth component.

    Returns:
        The sign used when summing components into net wealth.

    """
    return -1 if component in LIABILITY_COMPONENTS else 1
