"""Milestone-0 probe task: write the disclosure-safe registry presence report."""

import json
from pathlib import Path
from typing import Annotated

from pytask import Product

from soep_preparation.config import BLD, RAW_DATA_FILES, get_raw_data_file_names
from soep_preparation.wealth_imputation.probe import assemble_probe_report
from soep_preparation.wealth_imputation.registry_content import REGISTRY_ENTRIES


def task_wealth_imputation_probe(
    report_path: Annotated[Path, Product] = BLD
    / "wealth_imputation"
    / "milestone_0_probe.json",
) -> None:
    """Probe registry variables against available raw columns; write a JSON report.

    Reads only column names from each needed `RAW_DATA_FILES` entry and writes a
    disclosure-safe presence report (no row-level data).
    """
    needed = {entry.source_file for entry in REGISTRY_ENTRIES}
    available = {
        name: frozenset(RAW_DATA_FILES[name].load().columns)
        for name in get_raw_data_file_names()
        if name in needed
    }
    report = assemble_probe_report(REGISTRY_ENTRIES, available)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
