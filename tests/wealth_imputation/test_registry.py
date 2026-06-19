import pytest

from soep_preparation.wealth_imputation.components import CanonicalComponent
from soep_preparation.wealth_imputation.registry import (
    AggregationRule,
    VerificationStatus,
    WealthVariable,
    fail_if_unresolved_required,
)


def _entry(  # noqa: PLR0913
    *,
    status: VerificationStatus,
    required: bool,
    component: CanonicalComponent = CanonicalComponent.FINANCIAL_ASSETS,
    evidence: str = "codebook p.42",
    expected_min: float | None = 0.0,
    expected_max: float | None = None,
) -> WealthVariable:
    return WealthVariable(
        component=component,
        wave=2017,
        source_file="pl",
        raw_variable="f0100a",
        concept="financial assets value",
        unit="euro",
        universe="adults 17+",
        missing_codes=(-1, -2, -8),
        bracket_variable=None,
        ownership_flag=None,
        ownership_share_variable=None,
        level="person",
        aggregation_rule=AggregationRule.PERSON_SHARE_THEN_PLAIN_SUM,
        expected_min=expected_min,
        expected_max=expected_max,
        verification_status=status,
        verification_evidence=evidence,
        codebook_version="V41",
        required_for_release=required,
    )


def test_fail_if_unresolved_required_raises_for_required_unresolved_entry():
    entries = [_entry(status=VerificationStatus.UNRESOLVED, required=True)]
    with pytest.raises(
        ValueError, match="Release-critical registry entries are unresolved"
    ):
        fail_if_unresolved_required(entries)


def test_fail_if_unresolved_required_allows_unresolved_optional_entry():
    entries = [_entry(status=VerificationStatus.UNRESOLVED, required=False)]
    fail_if_unresolved_required(entries)  # no raise


def test_wealth_variable_is_frozen():
    entry = _entry(status=VerificationStatus.VERIFIED, required=True)
    with pytest.raises(AttributeError):
        entry.raw_variable = "x"  # ty: ignore[invalid-assignment]


def test_net_sign_is_minus_one_for_liability_component():
    entry = _entry(
        status=VerificationStatus.VERIFIED,
        required=True,
        component=CanonicalComponent.CONSUMER_DEBT,
    )
    assert entry.net_sign == -1


def test_net_sign_is_plus_one_for_asset_component():
    entry = _entry(status=VerificationStatus.VERIFIED, required=True)
    assert entry.net_sign == 1


def test_wealth_variable_rejects_inverted_expected_bounds():
    with pytest.raises(ValueError, match="exceeds expected_max"):
        _entry(
            status=VerificationStatus.INFERRED,
            required=False,
            expected_min=10.0,
            expected_max=1.0,
        )


def test_verified_entry_requires_non_empty_evidence():
    with pytest.raises(ValueError, match="requires non-empty evidence"):
        _entry(status=VerificationStatus.VERIFIED, required=True, evidence="")
