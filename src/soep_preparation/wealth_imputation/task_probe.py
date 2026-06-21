"""Milestone-0 probe task: write the disclosure-safe registry presence report.

Opt-in: the task is defined only when `RUN_WEALTH_IMPUTATION` is set (env var
`SOEP_WEALTH_IMPUTATION`, or the `pixi run wealth` task), so the default
`pixi run pytask` collects nothing from the wealth-imputation subsystem. Every
future wealth task module follows the same guard.
"""

import json
from pathlib import Path
from typing import Annotated

from pytask import Product

from soep_preparation.config import (
    BLD,
    RAW_DATA_FILES,
    RUN_WEALTH_IMPUTATION,
    SRC,
    get_raw_data_file_names,
)
from soep_preparation.wealth_imputation.probe import assemble_probe_report
from soep_preparation.wealth_imputation.registry_content import REGISTRY_ENTRIES

# First-party source modules whose content determines the report. Declared as task
# dependencies so editing any of them re-runs the probe without `--force`; the data
# catalogs and third-party imports are deliberately excluded.
_WEALTH_SRC = SRC / "wealth_imputation"
_SOURCE_DEPENDENCIES: tuple[Path, ...] = (
    _WEALTH_SRC / "probe.py",
    _WEALTH_SRC / "registry.py",
    _WEALTH_SRC / "registry_content.py",
    _WEALTH_SRC / "components.py",
)

if RUN_WEALTH_IMPUTATION:

    def task_wealth_imputation_probe(
        source_dependencies: tuple[Path, ...] = _SOURCE_DEPENDENCIES,
        report_path: Annotated[Path, Product] = BLD
        / "wealth_imputation"
        / "milestone_0_probe.json",
    ) -> None:
        """Probe registry variables against available raw columns; write a JSON report.

        Reads only column names from each needed `RAW_DATA_FILES` entry and writes a
        disclosure-safe presence report (no row-level data). `source_dependencies`
        carries the first-party modules whose edits should re-run the probe.
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
