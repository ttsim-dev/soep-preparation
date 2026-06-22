"""Milestone-0 probe task: write the disclosure-safe registry presence report.

Opt-in: the task is defined only when `RUN_WEALTH_IMPUTATION` is set (env var
`SOEP_WEALTH_IMPUTATION`, or the `pixi run wealth` task), so the default
`pixi run pytask` collects nothing from the wealth-imputation subsystem. Every
future wealth task module follows the same guard.
"""

import json
from pathlib import Path
from typing import Annotated

import pandas as pd
from pytask import Product

from soep_preparation.config import (
    BLD,
    RAW_DATA_FILES,
    RUN_WEALTH_IMPUTATION,
    SRC,
    get_raw_data_file_names,
)
from soep_preparation.wealth_imputation.probe import (
    assemble_probe_report,
    inventory_columns,
)
from soep_preparation.wealth_imputation.registry_content import REGISTRY_ENTRIES

# Source files to inventory so columns are chosen against the real V41 names.
# Covariates (ppathl, pgen, pequiv, pl, hgen, hl) plus hwealth, to confirm the
# household insurance/consumer-debt columns (h010h/c010h) before cleaning them.
_FEATURE_SOURCE_FILES = ("ppathl", "pgen", "pequiv", "pl", "hgen", "hl", "hwealth")

# Raw-data files the probe reads: the registry source files plus the covariate files.
_REGISTRY_SOURCE_FILES = tuple(
    sorted({entry.source_file for entry in REGISTRY_ENTRIES})
)
_PROBED_FILES = tuple(sorted({*_REGISTRY_SOURCE_FILES, *_FEATURE_SOURCE_FILES}))

# First-party source modules whose content determines the report. Declared as task
# dependencies so editing any of them re-runs the probe without `--force`; third-party
# imports are deliberately excluded.
_WEALTH_SRC = SRC / "wealth_imputation"
_SOURCE_DEPENDENCIES: tuple[Path, ...] = (
    _WEALTH_SRC / "probe.py",
    _WEALTH_SRC / "registry.py",
    _WEALTH_SRC / "registry_content.py",
    _WEALTH_SRC / "components.py",
)

if RUN_WEALTH_IMPUTATION:
    # Declare each probed catalog entry as a dependency so pytask runs the
    # convert_stata_to_pandas tasks first and injects the loaded frames; only files
    # actually present in the V41 catalog are wired.
    _CATALOG_FILES = set(get_raw_data_file_names())
    _DATA_INPUTS = {
        name: RAW_DATA_FILES[name] for name in _PROBED_FILES if name in _CATALOG_FILES
    }

    def task_wealth_imputation_probe(
        data_inputs: Annotated[dict[str, pd.DataFrame], _DATA_INPUTS],
        source_dependencies: tuple[Path, ...] = _SOURCE_DEPENDENCIES,
        report_path: Annotated[Path, Product] = BLD
        / "wealth_imputation"
        / "milestone_0_probe.json",
        feature_columns_path: Annotated[Path, Product] = BLD
        / "wealth_imputation"
        / "milestone_0_feature_columns.json",
    ) -> None:
        """Probe registry variables and inventory covariate columns; write JSON reports.

        Reads only column names from each injected `RAW_DATA_FILES` frame and writes two
        disclosure-safe reports (no row-level data): the registry presence report and a
        column inventory of the feature-source files. `data_inputs` declares the catalog
        dependencies so the raw-data conversions run first; `source_dependencies`
        carries the first-party modules whose edits should re-run the probe.
        """
        registry_files = set(_REGISTRY_SOURCE_FILES)
        available = {
            name: frozenset(frame.columns)
            for name, frame in data_inputs.items()
            if name in registry_files
        }
        report = assemble_probe_report(REGISTRY_ENTRIES, available)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

        feature_available = {
            name: frozenset(frame.columns)
            for name, frame in data_inputs.items()
            if name in _FEATURE_SOURCE_FILES
        }
        feature_columns = inventory_columns(feature_available)
        feature_columns_path.write_text(
            json.dumps(feature_columns, indent=2), encoding="utf-8"
        )
