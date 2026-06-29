"""Residual cross-fit task: validate the 2017 signed residual within the 2017 wave.

Opt-in like the imputation (`SOEP_WEALTH_IMPUTATION`, or `pixi run wealth`). The signed
reconciliation residual is fit only on 2017 (the one wave with the augmented official
total `n011h`), so the out-of-fold backtest that trains on 2002-2012 never exercises it.
This task supplies the missing in-sample evidence: a K-fold cross-fit within 2017 that
holds out each fold, fits the residual model and the donor residual pool on the others,
and PMM-draws the held-out fold's residual. It writes a disclosure-safe JSON of rank
correlation, sign accuracy, median absolute error, draw-band coverage, and the
residual's effect on the household total.

It validates only the *cross-sectional* residual prediction within 2017. The 2017->2022
temporal transport the production residual scenario relies on stays unvalidated -- there
is no second `n011h` wave to transport to -- so the residual-inclusive total remains a
labelled scenario, not a headline.
"""

import json
from pathlib import Path
from typing import Annotated

import numpy as np
import pandas as pd
from pytask import Product

from soep_preparation.config import (
    BLD,
    MODULES,
    RUN_WEALTH_IMPUTATION,
    SRC,
)
from soep_preparation.wealth_imputation.impute import residual_cross_fit_inputs
from soep_preparation.wealth_imputation.residual_backtest import (
    cross_fit_residual_draws,
    score_residual_cross_fit,
)

# Modules the cross-fit consumes (same as the imputation).
_BACKTEST_MODULES = ("hwealth", "pwealth", "pequiv", "pgen", "ppathl", "hgen")

# The residual-eligible wave (the only wave with the augmented official total) and the
# price level the residual is deflated into (the production target year).
_RESIDUAL_WAVE = 2017
_TARGET_YEAR = 2022

# Cross-fit settings (kept explicit so a re-run is reproducible).
_N_FOLDS = 5
_K = 10
_N_DRAWS = 200
_SEED = 0
_LEVEL = 0.9

_WEALTH_SRC = SRC / "wealth_imputation"
_SOURCE_DEPENDENCIES: tuple[Path, ...] = (
    _WEALTH_SRC / "residual_backtest.py",
    _WEALTH_SRC / "impute.py",
    _WEALTH_SRC / "residual_model.py",
    _WEALTH_SRC / "donors.py",
    _WEALTH_SRC / "features.py",
    _WEALTH_SRC / "deflation.py",
    _WEALTH_SRC / "market_indices.py",
    _WEALTH_SRC / "components.py",
)

if RUN_WEALTH_IMPUTATION:
    _MODULE_INPUTS = {name: MODULES[name] for name in _BACKTEST_MODULES}

    def task_wealth_residual_backtest(
        modules: Annotated[dict[str, pd.DataFrame], _MODULE_INPUTS],
        source_dependencies: tuple[Path, ...] = _SOURCE_DEPENDENCIES,
        report_path: Annotated[Path, Product] = BLD
        / "wealth_imputation"
        / "residual_backtest_2017.json",
    ) -> None:
        """Cross-fit the 2017 residual and write its disclosure-safe scorecard.

        Args:
            modules: Injected cleaned `MODULES` frames.
            source_dependencies: First-party modules whose edits re-run the task.
            report_path: Output JSON of the cross-fit metrics.
        """
        design, residual, component_total = residual_cross_fit_inputs(
            modules, wave=_RESIDUAL_WAVE, target_year=_TARGET_YEAR
        )
        result = cross_fit_residual_draws(
            design,
            residual,
            n_folds=_N_FOLDS,
            k=_K,
            n_draws=_N_DRAWS,
            rng=np.random.default_rng(seed=_SEED),
        )
        report = score_residual_cross_fit(
            residual, result.draws, component_total, level=_LEVEL
        )
        report["residual_wave"] = _RESIDUAL_WAVE
        report["target_year"] = _TARGET_YEAR
        report["n_folds"] = _N_FOLDS
        report["k"] = _K
        report["level"] = _LEVEL
        report["seed"] = _SEED
        # State the scope limit in the artefact itself: the cross-fit validates
        # cross-sectional residual prediction within 2017, never the 2017->2022 temporal
        # transport (there is no second n011h wave to transport to).
        report["validates"] = "within_2017_cross_sectional"
        report["temporal_transport_validated"] = False
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
