"""Imputation task: fit on the historical waves and write 2022 wealth intervals.

Opt-in like the probe: defined only when `RUN_WEALTH_IMPUTATION` is set (env var
`SOEP_WEALTH_IMPUTATION`, or `pixi run wealth`). It declares the cleaned `MODULES` it
consumes as dependencies, so pytask runs the cleaning first, then composes the tested
imputation pipeline and writes the 2022 household net-wealth intervals plus a
disclosure-safe run summary to `bld/wealth_imputation/`.
"""

import json
from pathlib import Path
from typing import Annotated

import pandas as pd
from pytask import Product

from soep_preparation.config import (
    BLD,
    MODULES,
    RUN_WEALTH_IMPUTATION,
    SRC,
)
from soep_preparation.wealth_imputation.impute import run_imputation

# Cleaned modules the imputation consumes: the wealth targets and the covariates.
_IMPUTE_MODULES = ("pwealth", "pequiv", "pgen", "ppathl", "hgen")

# Run settings (kept explicit so a re-run is reproducible).
_N_DRAWS = 200
_SEED = 0
_K = 10
_LEVEL = 0.9

_WEALTH_SRC = SRC / "wealth_imputation"
_SOURCE_DEPENDENCIES: tuple[Path, ...] = (
    _WEALTH_SRC / "impute.py",
    _WEALTH_SRC / "training.py",
    _WEALTH_SRC / "simulate.py",
    _WEALTH_SRC / "features.py",
    _WEALTH_SRC / "aggregate.py",
    _WEALTH_SRC / "amounts.py",
    _WEALTH_SRC / "donors.py",
    _WEALTH_SRC / "intervals.py",
    _WEALTH_SRC / "ownership_model.py",
    _WEALTH_SRC / "amount_model.py",
    _WEALTH_SRC / "transforms.py",
    _WEALTH_SRC / "components.py",
)

if RUN_WEALTH_IMPUTATION:
    _MODULE_INPUTS = {name: MODULES[name] for name in _IMPUTE_MODULES}

    def task_wealth_imputation_impute(
        modules: Annotated[dict[str, pd.DataFrame], _MODULE_INPUTS],
        source_dependencies: tuple[Path, ...] = _SOURCE_DEPENDENCIES,
        intervals_path: Annotated[Path, Product] = BLD
        / "wealth_imputation"
        / "household_wealth_2022.arrow",
        summary_path: Annotated[Path, Product] = BLD
        / "wealth_imputation"
        / "imputation_summary.json",
    ) -> None:
        """Run the provisional 2022 wealth imputation and write its outputs.

        Args:
            modules: Injected cleaned `MODULES` frames (declared dependencies).
            source_dependencies: First-party modules whose edits re-run the task.
            intervals_path: Output Feather file of per-household 2022 intervals.
            summary_path: Output JSON of the disclosure-safe run summary.
        """
        result = run_imputation(
            modules, n_draws=_N_DRAWS, seed=_SEED, k=_K, level=_LEVEL
        )
        intervals_path.parent.mkdir(parents=True, exist_ok=True)
        result.intervals.reset_index(drop=True).to_feather(intervals_path)
        summary_path.write_text(json.dumps(result.summary, indent=2), encoding="utf-8")
