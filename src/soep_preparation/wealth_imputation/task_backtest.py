"""Backtest task: hold out 2017, impute it from earlier waves, score against observed.

Opt-in like the imputation (`SOEP_WEALTH_IMPUTATION`, or `pixi run wealth`). It fits the
component models on the pre-2017 wealth waves, imputes 2017 out of fold, and compares
the imputed 2017 household component totals with the observed 2017 totals, writing a
disclosure-safe JSON of distribution, rank, and coverage metrics. This is the validation
the proxy otherwise lacks: it measures how well the donor models reproduce a real wealth
wave. The residual layer is not backtested -- the augmented official total exists only
from 2017, so there is no earlier outcome wave to fit it on.
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
from soep_preparation.wealth_imputation.backtest import backtest_report
from soep_preparation.wealth_imputation.impute import (
    observed_component_total,
    run_imputation,
)

# Modules the backtest consumes (same as the imputation).
_BACKTEST_MODULES = ("hwealth", "pwealth", "pequiv", "pgen", "ppathl", "hgen")

# Hold out 2017 (the latest observed wealth wave) and fit on the earlier waves.
_HOLDOUT_WAVE = 2017
_TRAINING_WAVES = (2002, 2007, 2012)
_HH_KEYS = ["hh_id", "survey_year"]

# Run settings (kept explicit so a re-run is reproducible).
_N_DRAWS = 200
_SEED = 0
_K = 10
_LEVEL = 0.9

_WEALTH_SRC = SRC / "wealth_imputation"
_SOURCE_DEPENDENCIES: tuple[Path, ...] = (
    _WEALTH_SRC / "backtest.py",
    _WEALTH_SRC / "impute.py",
    _WEALTH_SRC / "wealth_dynamics.py",
    _WEALTH_SRC / "simulate.py",
    _WEALTH_SRC / "training.py",
    _WEALTH_SRC / "features.py",
    _WEALTH_SRC / "residual_model.py",
    _WEALTH_SRC / "amount_model.py",
    _WEALTH_SRC / "ownership_model.py",
    _WEALTH_SRC / "deflation.py",
    _WEALTH_SRC / "market_indices.py",
    _WEALTH_SRC / "components.py",
)

if RUN_WEALTH_IMPUTATION:
    _MODULE_INPUTS = {name: MODULES[name] for name in _BACKTEST_MODULES}

    def task_wealth_imputation_backtest(
        modules: Annotated[dict[str, pd.DataFrame], _MODULE_INPUTS],
        source_dependencies: tuple[Path, ...] = _SOURCE_DEPENDENCIES,
        report_path: Annotated[Path, Product] = BLD
        / "wealth_imputation"
        / "backtest_2017_report.json",
    ) -> None:
        """Impute the held-out 2017 wave and write its disclosure-safe scorecard.

        Args:
            modules: Injected cleaned `MODULES` frames.
            source_dependencies: First-party modules whose edits re-run the task.
            report_path: Output JSON of the backtest metrics.
        """
        result = run_imputation(
            modules,
            n_draws=_N_DRAWS,
            seed=_SEED,
            k=_K,
            level=_LEVEL,
            prediction_wave=_HOLDOUT_WAVE,
            training_waves=_TRAINING_WAVES,
        )
        observed = observed_component_total(modules, _HOLDOUT_WAVE)
        comparison = result.intervals.merge(
            observed, on=_HH_KEYS, how="inner", validate="one_to_one"
        )
        report = backtest_report(comparison)
        report["n_recipients"] = int(result.summary["n_recipients"])
        report["n_training_heads"] = int(result.summary["n_training_heads"])
        report["holdout_wave"] = _HOLDOUT_WAVE
        report["training_waves"] = list(_TRAINING_WAVES)
        # Support loss from the complete-component-vector restriction on the truth
        # roster: households imputed in the holdout wave but excluded from the observed
        # truth because they did not observe every modelled component.
        report["n_fully_observed_truth"] = len(observed)
        report["n_truth_dropped_incomplete"] = int(
            result.summary["n_recipients"] - len(observed)
        )
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
