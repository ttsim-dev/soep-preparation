"""Transport backtest task: hold out each wealth wave, score temporal transport.

Opt-in like the imputation (`SOEP_WEALTH_IMPUTATION`, or `pixi run wealth`). Where the
single-wave backtest holds out only 2017, this holds out each of 2007/2012/2017 in turn
(training on the other wealth waves) and scores the imputed wave against both the
completed-component truth and the official `w011h` total (rank only). It writes a
disclosure-safe JSON whose `transport_stability` block shows how the headline metrics
vary across held-out waves -- the evidence the single-wave backtest cannot give for
whether the method transports temporally rather than fitting 2017 by accident.
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
from soep_preparation.wealth_imputation.transport_backtest import run_transport_backtest

# Modules the backtest consumes (same as the imputation).
_BACKTEST_MODULES = ("hwealth", "pwealth", "pequiv", "pgen", "ppathl", "hgen")

# The observed wealth waves. Each wave in `_HOLDOUT_WAVES` is held out and imputed from
# the others; 2002 stays a training anchor (predicting the earliest wave from only later
# ones is the least informative direction). Holding out 2017 reproduces the single-wave
# backtest's train/predict split exactly.
_ALL_WAVES = (2002, 2007, 2012, 2017)
_HOLDOUT_WAVES = (2007, 2012, 2017)

# Run settings (kept explicit so a re-run is reproducible).
_N_DRAWS = 200
_SEED = 0
_K = 10
_LEVEL = 0.9
_N_GROUPS = 5

_WEALTH_SRC = SRC / "wealth_imputation"
_SOURCE_DEPENDENCIES: tuple[Path, ...] = (
    _WEALTH_SRC / "transport_backtest.py",
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

    def task_wealth_imputation_transport_backtest(
        modules: Annotated[dict[str, pd.DataFrame], _MODULE_INPUTS],
        source_dependencies: tuple[Path, ...] = _SOURCE_DEPENDENCIES,
        report_path: Annotated[Path, Product] = BLD
        / "wealth_imputation"
        / "transport_backtest_report.json",
    ) -> None:
        """Score each held-out wealth wave and write the transport scorecard.

        Args:
            modules: Injected cleaned `MODULES` frames.
            source_dependencies: First-party modules whose edits re-run the task.
            report_path: Output JSON of the per-wave metrics and stability summary.
        """
        report = run_transport_backtest(
            modules,
            holdout_waves=_HOLDOUT_WAVES,
            all_waves=_ALL_WAVES,
            n_draws=_N_DRAWS,
            seed=_SEED,
            k=_K,
            level=_LEVEL,
            n_groups=_N_GROUPS,
        )
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
