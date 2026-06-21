import json

from soep_preparation.wealth_imputation.components import CanonicalComponent
from soep_preparation.wealth_imputation.probe import assemble_probe_report
from soep_preparation.wealth_imputation.registry import (
    AggregationRule,
    VerificationStatus,
    WealthVariable,
)

_EXPECTED_ENTRY_KEYS = {
    "component",
    "wave",
    "source_file",
    "raw_variable",
    "present",
    "verification_status",
}
_EXPECTED_SUMMARY_KEYS = {
    "n_entries",
    "n_present",
    "n_missing",
    "n_unresolved_required",
}


def _entry(raw_variable: str, *, required: bool) -> WealthVariable:
    return WealthVariable(
        component=CanonicalComponent.FINANCIAL_ASSETS,
        wave=2017,
        source_file="pl",
        raw_variable=raw_variable,
        concept="financial assets",
        unit="euro",
        universe="adults 17+",
        missing_codes=(-1,),
        bracket_variable=None,
        ownership_flag=None,
        ownership_share_variable=None,
        level="person",
        aggregation_rule=AggregationRule.PERSON_DIRECT_PLAIN_SUM,
        expected_min=0.0,
        expected_max=None,
        verification_status=VerificationStatus.UNRESOLVED,
        verification_evidence="",
        codebook_version="V41",
        required_for_release=required,
    )


def test_assemble_probe_report_flags_present_and_missing_without_leaking_data():
    entries = [
        _entry("present_var", required=True),
        _entry("absent_var", required=True),
    ]
    available = {"pl": frozenset({"present_var", "other"})}
    report = assemble_probe_report(entries, available)
    assert report["summary"]["n_present"] == 1
    assert report["summary"]["n_missing"] == 1
    assert report["summary"]["n_unresolved_required"] == 2  # noqa: PLR2004
    presence = {row["raw_variable"]: row["present"] for row in report["entries"]}
    assert presence == {"present_var": True, "absent_var": False}


def test_assemble_probe_report_has_exact_disclosure_safe_schema():
    report = assemble_probe_report(
        [_entry("present_var", required=True)], {"pl": frozenset({"present_var"})}
    )
    assert set(report.keys()) == {"entries", "summary", "observed_columns"}
    assert set(report["summary"].keys()) == _EXPECTED_SUMMARY_KEYS
    assert set(report["entries"][0].keys()) == _EXPECTED_ENTRY_KEYS
    # Disclosure-safe: the whole report serialises to JSON (only primitives/keys).
    json.dumps(report)


def test_assemble_probe_report_lists_only_wealth_pattern_columns():
    """`observed_columns` surfaces DIW wealth names and drops identifiers/free text."""
    available = {"pwealth": frozenset({"p0100a", "p10000", "pid", "syear", "comment"})}
    report = assemble_probe_report([_entry("p0100a", required=True)], available)
    assert report["observed_columns"] == {"pwealth": ["p0100a", "p10000"]}
