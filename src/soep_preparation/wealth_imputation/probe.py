"""Milestone-0 source probe: confirm registry variables exist, disclosure-safe.

Reports only registry keys, presence booleans, and the wealth-pattern *column names*
present in each probed source file — never row-level data — so it is safe to read
from logs/artifacts under the no-data-access rule.
"""

import re
from collections.abc import Mapping, Sequence

import pandas as pd

from soep_preparation.wealth_imputation.registry import (
    VerificationStatus,
    WealthVariable,
)

_YEAR_COLUMN = "syear"

# DIW generated wealth variables: a single component letter, four or five digits,
# and an optional implicate suffix `a`-`e` (e.g. `p0100a`, `p0010a`, `w0101a`,
# `p10000`). Matching column names lets us discover the real V41 names for any
# component whose expected name probes absent.
_WEALTH_COLUMN_PATTERN = re.compile(r"^[a-z]\d{4,5}[a-e]?$")


def inventory_columns(
    available_columns: Mapping[str, frozenset[str]],
) -> dict[str, list[str]]:
    """Return the sorted column names of each probed source file.

    Disclosure-safe: column names only, no row-level data. Used to discover the real
    covariate variable names in the feature-source files (`ppathl`, `pgen`, ...) so
    model predictors are chosen against confirmed names rather than guessed.

    Args:
        available_columns: Source-file name -> set of its column names.

    Returns:
        Source-file name -> sorted list of its column names.
    """
    return {
        source_file: sorted(columns)
        for source_file, columns in available_columns.items()
    }


def probe_wealth_wave_population(
    frames: Mapping[str, pd.DataFrame], *, years: Sequence[int]
) -> dict:
    """Count populated wealth cells per source file and survey year.

    Answers two questions the column-presence probe cannot: whether a wealth wave (e.g.
    2022) actually carries values, and whether it is multiply imputed or raw-only. For
    each wealth-pattern column the non-null count is reported per year; comparing the
    implicate `a` count with the `b`-`e` counts distinguishes a DIW multiply-imputed
    wave (all implicates populated) from raw answers (only `a`, with item-NR holes).

    Disclosure-safe: only aggregate counts and row totals are returned, never a
    row-level value.

    Args:
        frames: Source-file name -> raw frame (must carry a `syear` column to split by
            year; a frame without it reports zero rows for every year).
        years: Survey years to report.

    Returns:
        Source-file name -> `{str(year): {"n_rows": int, "wealth_columns_non_null":
        {column: int}}}`.
    """
    report = {}
    for source_file, frame in frames.items():
        wealth_columns = sorted(
            column
            for column in frame.columns
            if _WEALTH_COLUMN_PATTERN.fullmatch(column)
        )
        has_year = _YEAR_COLUMN in frame.columns
        per_year = {}
        for year in years:
            subset = frame[frame[_YEAR_COLUMN] == year] if has_year else frame.iloc[:0]
            per_year[str(year)] = {
                "n_rows": len(subset),
                "wealth_columns_non_null": {
                    column: int(subset[column].notna().sum())
                    for column in wealth_columns
                },
            }
        report[source_file] = per_year
    return report


def assemble_probe_report(
    entries: Sequence[WealthVariable],
    available_columns: Mapping[str, frozenset[str]],
) -> dict:
    """Build a disclosure-safe presence report for registry entries.

    Args:
        entries: Registry entries to probe.
        available_columns: Source-file name → set of its column names.

    Returns:
        A dict with per-entry presence flags, a numeric summary, and the
        wealth-pattern column names observed in each probed source file. Contains
        no row-level data.
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
    observed_columns = {
        source_file: sorted(
            column for column in columns if _WEALTH_COLUMN_PATTERN.fullmatch(column)
        )
        for source_file, columns in available_columns.items()
    }
    return {
        "entries": rows,
        "summary": summary,
        "observed_columns": observed_columns,
    }
