"""Replicate task: build the component-only 2022 net-wealth projection replicates.

Opt-in like the other wealth tasks (env var `SOEP_WEALTH_IMPUTATION`, or
`pixi run wealth`). It calibrates the transport layer from the official cross-wave
aggregates, runs the replicate engine -- five bootstrap refits, each contributing one
predictive draw plus a systematic transport shock -- and writes the released `a`-`e`
projection draws plus a disclosure-safe summary (calibration inputs, Monte-Carlo error,
and the metadata guards).

The released columns are the six-component net-wealth total (they omit the
reconciliation residual) and are lettered `a`-`e` as exchangeable projection draws, not
DIW donor
implicates; the metadata block records that the release is not Rubin-valid. Each draw is
a single predictive draw -- the five replicates *are* the draws, so donor-draw
uncertainty is carried across them rather than averaged into a point estimate -- while
the parameter (bootstrap) and transport layers enter through the between-replicate
spread.
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
from soep_preparation.wealth_imputation.replicates import (
    build_implicates_metadata,
    impute_replicates,
    official_wealth_aggregates,
    replicate_mc_summary,
    select_released_implicates,
    transport_scale_from_official_aggregates,
)

# Cleaned modules the imputation consumes: household + person wealth and the covariates.
_IMPUTE_MODULES = ("hwealth", "pwealth", "pequiv", "pgen", "ppathl", "hgen")

# One predictive draw per replicate; five released, matching the DIW implicate count.
_N_REPLICATES = 5
_N_RELEASED = 5
_N_DRAWS = 1
_SEED = 0
_K = 10

# Official all-wave net-wealth total (`w011h`, implicate a) and the wealth waves it
# calibrates the transport scale from.
_OFFICIAL_TOTAL_COLUMN = "hh_net_overall_wealth_a"
_WEALTH_WAVES = (2002, 2007, 2012, 2017)

_WEALTH_SRC = SRC / "wealth_imputation"
_SOURCE_DEPENDENCIES: tuple[Path, ...] = (
    _WEALTH_SRC / "replicates.py",
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
    _WEALTH_SRC / "residual_model.py",
    _WEALTH_SRC / "transforms.py",
    _WEALTH_SRC / "deflation.py",
    _WEALTH_SRC / "market_indices.py",
    _WEALTH_SRC / "components.py",
)

if RUN_WEALTH_IMPUTATION:
    _MODULE_INPUTS = {name: MODULES[name] for name in _IMPUTE_MODULES}

    def task_wealth_imputation_replicates(
        modules: Annotated[dict[str, pd.DataFrame], _MODULE_INPUTS],
        source_dependencies: tuple[Path, ...] = _SOURCE_DEPENDENCIES,
        implicates_path: Annotated[Path, Product] = BLD
        / "wealth_imputation"
        / "household_wealth_2022_component_only_projection_replicates.arrow",
        summary_path: Annotated[Path, Product] = BLD
        / "wealth_imputation"
        / "projection_replicates_summary.json",
    ) -> None:
        """Build and write the component-only 2022 net-wealth projection replicates.

        Args:
            modules: Injected cleaned `MODULES` frames (declared dependencies).
            source_dependencies: First-party modules whose edits re-run the task.
            implicates_path: Output Feather file of the released projection draws.
            summary_path: Output JSON of the calibration, Monte-Carlo error, and guards.
        """
        aggregates = official_wealth_aggregates(
            modules["hwealth"],
            total_column=_OFFICIAL_TOTAL_COLUMN,
            waves=_WEALTH_WAVES,
        )
        transport_log_scale = transport_scale_from_official_aggregates(
            aggregates["wave_aggregates"]
        )
        total_scale = aggregates["median_absolute_total"]

        replicates = impute_replicates(
            modules,
            n_replicates=_N_REPLICATES,
            base_seed=_SEED,
            transport_log_scale=transport_log_scale,
            total_scale=total_scale,
            n_draws=_N_DRAWS,
            k=_K,
        )
        released = select_released_implicates(replicates, n_released=_N_RELEASED)

        summary = {
            "calibration": {
                # Unweighted row-sum aggregates (no SOEP design weights), consistent
                # with the rest of the harness; a sample-total-growth prior, not a
                # validated population transport-error scale.
                "aggregate_basis": "unweighted_sample_total",
                "wave_aggregates": {
                    str(year): value
                    for year, value in aggregates["wave_aggregates"].items()
                },
                "median_absolute_total": aggregates["median_absolute_total"],
                "transport_log_scale": transport_log_scale,
            },
            "monte_carlo_error": replicate_mc_summary(replicates),
            "metadata": build_implicates_metadata(
                n_replicates=_N_REPLICATES,
                n_released=_N_RELEASED,
                transport_log_scale=transport_log_scale,
                total_scale=total_scale,
            ),
        }

        released.to_feather(implicates_path)
        summary_path.write_text(json.dumps(summary, indent=2))
