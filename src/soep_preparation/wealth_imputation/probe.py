"""Milestone-0 source probe: confirm registry variables exist, disclosure-safe.

Reports only registry keys and presence booleans — never row-level data — so it is
safe to read from logs/artifacts under the no-data-access rule.
"""

from collections.abc import Mapping, Sequence

from soep_preparation.wealth_imputation.registry import (
    VerificationStatus,
    WealthVariable,
)


def assemble_probe_report(
    entries: Sequence[WealthVariable],
    available_columns: Mapping[str, frozenset[str]],
) -> dict:
    """Build a disclosure-safe presence report for registry entries.

    Args:
        entries: Registry entries to probe.
        available_columns: Source-file name → set of its column names.

    Returns:
        A dict with per-entry presence flags and a numeric summary. Contains no
        row-level data.
    """
    rows = []
    for entry in entries:
        present = entry.raw_variable in available_columns.get(
            entry.source_file, frozenset()
        )
        rows.append(
            {
                "component": entry.component.value,
                "wave": entry.wave,
                "source_file": entry.source_file,
                "raw_variable": entry.raw_variable,
                "present": present,
                "verification_status": entry.verification_status.value,
            }
        )
    n_present = sum(row["present"] for row in rows)
    n_unresolved_required = sum(
        entry.required_for_release
        and entry.verification_status is VerificationStatus.UNRESOLVED
        for entry in entries
    )
    summary = {
        "n_entries": len(rows),
        "n_present": n_present,
        "n_missing": len(rows) - n_present,
        "n_unresolved_required": n_unresolved_required,
    }
    return {"entries": rows, "summary": summary}
