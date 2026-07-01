import json

import pandas as pd

from soep_preparation.wealth_imputation.components import CanonicalComponent
from soep_preparation.wealth_imputation.probe import (
    assemble_probe_report,
    inventory_columns,
    probe_wealth_wave_population,
)
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
    assert report["summary"]["n_unresolved_required"] == 2
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


def test_inventory_columns_returns_sorted_names_per_source():
    """Each source file maps to its column names, sorted, with no row data."""
    available = {
        "pgen": frozenset({"pgisced11", "pgemplst", "pid"}),
        "hgen": frozenset({"hgowner", "hid"}),
    }
    assert inventory_columns(available) == {
        "pgen": ["pgemplst", "pgisced11", "pid"],
        "hgen": ["hgowner", "hid"],
    }


def test_assemble_probe_report_lists_only_wealth_pattern_columns():
    """`observed_columns` surfaces DIW wealth names and drops identifiers/free text."""
    available = {"pwealth": frozenset({"p0100a", "p10000", "pid", "syear", "comment"})}
    report = assemble_probe_report([_entry("p0100a", required=True)], available)
    assert report["observed_columns"] == {"pwealth": ["p0100a", "p10000"]}


def _wealth_frame() -> pd.DataFrame:
    """Raw-style wealth frame: implicate `a` populated, `b` empty (raw, not imputed)."""
    return pd.DataFrame(
        {
            "syear": [2017, 2017, 2022, 2022],
            "p0100a": [100.0, None, 200.0, 300.0],
            "p0100b": [110.0, 120.0, None, None],
            "notwealth": [1, 2, 3, 4],
        }
    )


def test_probe_wealth_wave_population_counts_non_null_in_the_target_year():
    """The probe counts non-null wealth cells per source and survey year."""
    frame = _wealth_frame()
    report = probe_wealth_wave_population({"pwealth": frame}, years=(2017, 2022))
    expected = int(frame.query("syear == 2022")["p0100a"].notna().sum())
    assert report["pwealth"]["2022"]["wealth_columns_non_null"]["p0100a"] == expected


def test_probe_wealth_wave_population_flags_an_empty_implicate():
    """An implicate empty in the target year reads as zero (raw, not imputed)."""
    report = probe_wealth_wave_population(
        {"pwealth": _wealth_frame()}, years=(2017, 2022)
    )
    assert report["pwealth"]["2022"]["wealth_columns_non_null"]["p0100b"] == 0
