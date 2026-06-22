"""Run the provisional 2022 household-wealth imputation end to end.

This is the v1 wiring that turns the cleaned modules into 2022 household net-wealth
intervals, composing the tested building blocks:

1. assemble the person-level feature matrix and merge the cleaned `pwealth` component
   values onto it;
2. represent each household by its oldest member (`select_household_heads`) so totals
   aggregate one row per household without summing jointly-held amounts SOEP-Core
   cannot share-split;
3. for each available component, fit ownership + amount models on the historical
   waves and build a draw config for the 2022 recipients;
4. simulate joint draws into household net-total intervals, then shift by the mean
   historical residual to the official total (the unmodelled other-real-estate +
   business contribution).

v1 simplifications (documented for refinement): continuous predictors only; implicate
`a` as the representative value; one head per household; a single mean-residual offset
rather than a modelled residual; no asset-class donor deflation yet.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import numpy as np
import pandas as pd

from soep_preparation.wealth_imputation.components import (
    CanonicalComponent,
    component_sign,
)
from soep_preparation.wealth_imputation.features import (
    FEATURE_SPECS,
    assemble_feature_matrix,
    encode_design_matrix,
    select_household_heads,
)
from soep_preparation.wealth_imputation.simulate import simulate_household_totals
from soep_preparation.wealth_imputation.training import (
    build_component_config,
    derive_ownership,
    fit_component_models,
)

_PREDICTION_WAVE = 2022
_MIN_TRAINING_ROWS = 5
_MIN_OWNERS = 2
_KEYS = ["p_id", "hh_id", "survey_year"]

# Cleaned pwealth columns (implicate `a`) backing each available component.
_COMPONENT_COLUMNS: dict[CanonicalComponent, str] = {
    CanonicalComponent.OWNER_OCCUPIED_PROPERTY_GROSS: (
        "property_value_primary_residence_a"
    ),
    CanonicalComponent.FINANCIAL_ASSETS: "financial_assets_value_a",
    CanonicalComponent.PRIVATE_PENSION: "private_insurances_value_a",
    CanonicalComponent.VEHICLES: "vehicles_value_a",
    CanonicalComponent.CONSUMER_DEBT: "consumer_debt_value_a",
}
_OFFICIAL_TOTAL_COLUMN = "net_overall_wealth_including_vehicles_and_student_loans_a"


@dataclass(frozen=True)
class ImputationResult:
    """The 2022 household-wealth intervals and a disclosure-safe run summary."""

    intervals: pd.DataFrame
    """Columns `hh_id`, `survey_year`, `point_estimate`, `lower`, `upper`."""
    summary: dict
    """Counts and choices for the run (no row-level data)."""


def run_imputation(
    modules: Mapping[str, pd.DataFrame],
    *,
    n_draws: int,
    seed: int,
    k: int,
    level: float = 0.9,
) -> ImputationResult:
    """Impute 2022 household net wealth as donor-based intervals.

    Args:
        modules: Cleaned `MODULES` frames; must include `pwealth` and the feature
            modules used by `assemble_feature_matrix`.
        n_draws: Number of complete joint draws.
        seed: Seed for model fitting and the draw RNG.
        k: Nearest-donor count for PMM (clipped to each component's owner count).
        level: Central coverage of the reported intervals.

    Returns:
        An `ImputationResult` with the 2022 intervals and a run summary.

    Raises:
        ValueError: If there are no 2022 recipients or no component can be fit.
    """
    heads = _household_heads_with_wealth(modules)
    predictor_columns = [
        spec.column for spec in FEATURE_SPECS if spec.kind == "continuous"
    ]
    training = heads[heads["survey_year"] < _PREDICTION_WAVE]
    recipients = heads[heads["survey_year"] == _PREDICTION_WAVE].reset_index(drop=True)
    if recipients.empty:
        msg = "No 2022 recipients found; cannot impute the prediction wave."
        raise ValueError(msg)

    recipient_design = encode_design_matrix(recipients, predictor_columns)
    recipient_keys = recipients[_KEYS]
    configs = []
    used: list[CanonicalComponent] = []
    skipped: list[str] = []
    for component, column in _COMPONENT_COLUMNS.items():
        trained = training.dropna(subset=[column])
        values = trained[column].to_numpy(dtype="float64")
        owners = int((values > 0.0).sum())
        if (
            len(trained) < _MIN_TRAINING_ROWS
            or set(derive_ownership(values).tolist()) != {0, 1}
            or owners < _MIN_OWNERS
        ):
            skipped.append(component.value)
            continue
        design = encode_design_matrix(trained, predictor_columns)
        models = fit_component_models(design, values, seed=seed)
        owner_mask = values > 0.0
        configs.append(
            build_component_config(
                component=component,
                models=models,
                recipient_features=recipient_design,
                recipient_shares=np.ones(len(recipient_keys), dtype="float64"),
                donor_features=design[owner_mask],
                donor_values=values[owner_mask],
                k=min(k, owners),
            )
        )
        used.append(component)
    if not configs:
        msg = "No wealth component could be fit on the historical waves."
        raise ValueError(msg)

    intervals = simulate_household_totals(
        recipient_keys,
        configs,
        n_draws=n_draws,
        rng=np.random.default_rng(seed),
        level=level,
    )
    mean_residual = _mean_training_residual(training, used)
    for column in ("point_estimate", "lower", "upper"):
        intervals[column] = intervals[column] + mean_residual
    summary = {
        "n_recipients": len(recipient_keys),
        "n_training_heads": len(training),
        "components_used": [component.value for component in used],
        "components_skipped": skipped,
        "mean_residual": mean_residual,
        "n_draws": n_draws,
        "k": k,
        "level": level,
    }
    return ImputationResult(intervals=intervals, summary=summary)


def _household_heads_with_wealth(modules: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    features = assemble_feature_matrix(modules)
    wealth_columns = [*_COMPONENT_COLUMNS.values(), _OFFICIAL_TOTAL_COLUMN]
    wealth = modules["pwealth"][[*_KEYS, *wealth_columns]]
    panel = features.merge(wealth, on=_KEYS, how="left")
    return select_household_heads(panel)


def _mean_training_residual(
    training: pd.DataFrame, used: Sequence[CanonicalComponent]
) -> float:
    have_total = training.dropna(subset=[_OFFICIAL_TOTAL_COLUMN])
    if have_total.empty:
        return 0.0
    modelled = np.zeros(len(have_total), dtype="float64")
    for component in used:
        values = (
            pd.to_numeric(have_total[_COMPONENT_COLUMNS[component]], errors="coerce")
            .fillna(0.0)
            .to_numpy(dtype="float64")
        )
        modelled += component_sign(component) * values
    official = pd.to_numeric(
        have_total[_OFFICIAL_TOTAL_COLUMN], errors="coerce"
    ).to_numpy(dtype="float64")
    residual = official - modelled
    residual = residual[np.isfinite(residual)]
    return float(np.mean(residual)) if residual.size else 0.0
