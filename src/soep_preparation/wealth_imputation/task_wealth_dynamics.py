"""Dynamics task: per-wave wealth distribution and 5-year mobility, disclosure-safe.

Opt-in like the imputation (`SOEP_WEALTH_IMPUTATION`, or `pixi run wealth`). It consumes
the cleaned `hwealth` (official household totals for the wealth waves) and `ppathl` (the
person-household roster), plus the imputed 2022 intervals produced by the imputation
task, and writes a single JSON of aggregates -- per-wave distribution statistics and
quintile transition matrices across the four 5-year horizons. No row-level value leaves
the secure environment.
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
from soep_preparation.wealth_imputation.impute import _OFFICIAL_TOTAL_COLUMN
from soep_preparation.wealth_imputation.wealth_dynamics import build_dynamics_report

# Actual wealth-module waves carried by `hwealth`; 2022 is appended from the imputation.
_ACTUAL_WAVES = (2002, 2007, 2012, 2017)
_IMPUTED_WAVE = 2022
_WAVES = (*_ACTUAL_WAVES, _IMPUTED_WAVE)
_N_GROUPS = 5
_MIN_CELL = 30

_DYNAMICS_MODULES = ("hwealth", "ppathl")
_WEALTH_SRC = SRC / "wealth_imputation"
_SOURCE_DEPENDENCIES: tuple[Path, ...] = (
    _WEALTH_SRC / "wealth_dynamics.py",
    _WEALTH_SRC / "impute.py",
)
_IMPUTED_INTERVALS = BLD / "wealth_imputation" / "household_wealth_2022.arrow"


def _assemble_household_wealth(
    hwealth: pd.DataFrame, imputed: pd.DataFrame
) -> pd.DataFrame:
    """Stack official household net wealth (actual waves) and the imputed 2022 proxy."""
    actual = (
        hwealth.loc[
            hwealth["survey_year"].isin(_ACTUAL_WAVES),
            ["hh_id", "survey_year", _OFFICIAL_TOTAL_COLUMN],
        ]
        .rename(columns={_OFFICIAL_TOTAL_COLUMN: "net_wealth"})
        .dropna(subset=["net_wealth"])
    )
    proxy = imputed[["hh_id", "survey_year", "point_estimate"]].rename(
        columns={"point_estimate": "net_wealth"}
    )
    combined = pd.concat([actual, proxy], ignore_index=True)
    combined["net_wealth"] = combined["net_wealth"].astype("float64")
    return combined


if RUN_WEALTH_IMPUTATION:
    _MODULE_INPUTS = {name: MODULES[name] for name in _DYNAMICS_MODULES}

    def task_wealth_imputation_dynamics(
        modules: Annotated[dict[str, pd.DataFrame], _MODULE_INPUTS],
        imputed_intervals: Path = _IMPUTED_INTERVALS,
        source_dependencies: tuple[Path, ...] = _SOURCE_DEPENDENCIES,
        report_path: Annotated[Path, Product] = BLD
        / "wealth_imputation"
        / "wealth_dynamics_report.json",
    ) -> None:
        """Write per-wave distribution and 5-year mobility aggregates.

        Args:
            modules: Injected cleaned `hwealth` and `ppathl` frames.
            imputed_intervals: The imputation task's per-household 2022 intervals.
            source_dependencies: First-party modules whose edits re-run the task.
            report_path: Output JSON of the disclosure-safe dynamics report.
        """
        imputed = pd.read_feather(imputed_intervals)
        household_wealth = _assemble_household_wealth(modules["hwealth"], imputed)
        roster = modules["ppathl"][["p_id", "hh_id", "survey_year"]]
        report = build_dynamics_report(
            household_wealth,
            roster,
            waves=_WAVES,
            n_groups=_N_GROUPS,
            min_cell=_MIN_CELL,
        )
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
