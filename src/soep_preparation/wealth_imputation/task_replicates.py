"""Replicate task: build the component-only 2022 net-wealth projection replicates.

Opt-in like the other wealth tasks (env var `SOEP_WEALTH_IMPUTATION`, or
`pixi run wealth`). It calibrates the transport scale from the official cross-wave
aggregates, runs the replicate engine -- five bootstrap refits, each cycling a distinct
DIW donor implicate and contributing one predictive draw -- and writes the released
`a`-`e` projection draws plus a disclosure-safe summary.

The released `component_only_net_wealth_2022_*` columns are the six-component net-wealth
total (they omit the reconciliation residual), lettered `a`-`e` as projection replicates
built from DIW donor implicates -- not DIW's own implicates; the metadata block records
that the release is not Rubin-valid. Each is a single predictive draw -- the five
replicates *are* the draws, so donor-draw uncertainty is carried across them -- and
their spread reflects the bootstrap and donor-implicate layers. The transport shock is
reported *separately*: the `component_only_net_wealth_2022_transport_scenario_*` columns
and the summary's `transport_scenario` block are a labelled macro-sensitivity axis (a
calibrated, unvalidated prior that shifts the euro-scale level), kept out of the
projection spread so the latter stays an interpretable between-replicate uncertainty.
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
    transport_scenario_summary,
)

# Cleaned modules the imputation consumes: household + person wealth, the covariates,
# and `hpathl` for the household design weight used to calibrate the transport scale.
_IMPUTE_MODULES = ("hwealth", "pwealth", "pequiv", "pgen", "ppathl", "hgen", "hpathl")

# One predictive draw per replicate; five released, matching the DIW implicate count.
# Each replicate imputes from a distinct DIW donor implicate a-e, so the
# between-replicate spread also prices DIW's own imputation uncertainty (layer a).
_N_REPLICATES = 5
_N_RELEASED = 5
_N_DRAWS = 1
_SEED = 0
_K = 10
_DONOR_IMPLICATES = ("a", "b", "c", "d", "e")

_WEIGHT_COLUMN = "hh_weighting_factor"

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
        household_wealth = pd.merge(
            modules["hwealth"],
            modules["hpathl"][["hh_id", "survey_year", _WEIGHT_COLUMN]],
            on=["hh_id", "survey_year"],
            how="left",
        )
        aggregates = official_wealth_aggregates(
            household_wealth,
            total_column=_OFFICIAL_TOTAL_COLUMN,
            waves=_WEALTH_WAVES,
            weight_column=_WEIGHT_COLUMN,
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
            donor_implicates=_DONOR_IMPLICATES,
        )
        # Release the no-transport projection replicates (the interpretable object) and
        # the transport-scenario draws as a second, clearly-named column set, so an
        # analyst can see the macro-sensitivity axis without it being folded into the
        # projection spread.
        released = select_released_implicates(
            replicates.projection,
            n_released=_N_RELEASED,
            name="component_only_net_wealth_2022",
        ).merge(
            select_released_implicates(
                replicates.transport_scenario,
                n_released=_N_RELEASED,
                name="component_only_net_wealth_2022_transport_scenario",
            ),
            on="hh_id",
            how="left",
        )

        summary = {
            "calibration": {
                # Design-weighted population totals (household weight `hhrf`), so the
                # cross-wave log-growth reflects population wealth, not sample size. It
                # is still an empirical prior, not a validated transport-error scale.
                "aggregate_basis": "design_weighted_population_total",
                "wave_aggregates": {
                    str(year): value
                    for year, value in aggregates["wave_aggregates"].items()
                },
                "median_absolute_total": aggregates["median_absolute_total"],
                "transport_log_scale": transport_log_scale,
            },
            # The interpretable between-replicate spread (bootstrap, donor-implicate,
            # donor-draw), free of the transport prior.
            "projection_replicates": replicate_mc_summary(replicates.projection),
            # The transport shock as a separate labelled scenario axis: its own spread
            # plus the isolated euro-scale level shift it induces (base held fixed).
            "transport_scenario": {
                **replicate_mc_summary(replicates.transport_scenario),
                **transport_scenario_summary(replicates),
            },
            "metadata": build_implicates_metadata(
                n_replicates=_N_REPLICATES,
                n_released=_N_RELEASED,
                transport_log_scale=transport_log_scale,
                total_scale=total_scale,
                donor_implicates_propagated=len(set(_DONOR_IMPLICATES)) > 1,
            ),
        }

        released.to_feather(implicates_path)
        summary_path.write_text(json.dumps(summary, indent=2))
