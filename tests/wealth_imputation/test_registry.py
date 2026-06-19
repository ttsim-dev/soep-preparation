import pytest

from soep_preparation.wealth_imputation.components import CanonicalComponent
from soep_preparation.wealth_imputation.registry import (
    VerificationStatus,
    WealthVariable,
    fail_if_unresolved_required,
)


def _entry(*, status: VerificationStatus, required: bool) -> WealthVariable:
    return WealthVariable(
        component=CanonicalComponent.FINANCIAL_ASSETS,
        wave=2017,
        source_file="pl",
        raw_variable="f0100a",
        concept="financial assets value",
        unit="euro",
        sign=1,
        universe="adults 17+",
        missing_codes=(-1, -2, -8),
        bracket_variable=None,
        ownership_flag=None,
        ownership_share_variable=None,
        level="person",
        aggregation_rule="sum_member_shares",
        expected_min=0.0,
        expected_max=None,
        verification_status=status,
        verification_evidence="codebook p.42",
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
