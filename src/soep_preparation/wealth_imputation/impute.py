"""Run the provisional 2022 household-wealth imputation end to end.

Targets are the DIW-aggregated **household** totals from cleaned `hwealth` (correctly
share-summed, so no person-share double-counting), modelled per available component
with a residual to the official total. The pipeline composes the tested building
blocks:

1. assemble the person feature matrix, attach each person's lagged prior-wave wealth,
   and represent each household by its oldest member (`select_household_heads`);
2. merge the household component targets onto the heads;
3. restrict training to the wealth waves, fit ownership + amount models per component
   on continuous + one-hot categorical predictors, with cross-wave donor values
   deflated to 2022 terms by asset-class indices;
4. simulate joint draws into household net-total intervals, shifted by the mean
   historical residual to the official total.

v1->v2 status: household-level targets, wealth-wave training, categorical encoding,
lagged wealth, and MSCI deflation of the financial component are wired. Remaining
approximations (documented): property/vehicles donors are not deflated (no verified
house-price index on file yet); the residual offset is nominal and flat; implicate `a`
is the representative value; household property is taken net of mortgage.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import numpy as np
import pandas as pd

from soep_preparation.wealth_imputation.components import (
    CanonicalComponent,
    component_sign,
)
from soep_preparation.wealth_imputation.deflation import deflate_donor_values
from soep_preparation.wealth_imputation.features import (
    FEATURE_SPECS,
    assemble_feature_matrix,
    encode_features,
    fit_categorical_encoder,
    lagged_wealth,
    select_household_heads,
)
from soep_preparation.wealth_imputation.market_indices import MSCI_WORLD_INDEX
from soep_preparation.wealth_imputation.simulate import simulate_household_totals
from soep_preparation.wealth_imputation.training import (
    build_component_config,
    fit_component_models,
)

_PREDICTION_WAVE = 2022
_WEALTH_WAVES = (2002, 2007, 2012, 2017)
_MIN_TRAINING_ROWS = 5
_MIN_OWNERS = 2
_KEYS = ["p_id", "hh_id", "survey_year"]
_HH_KEYS = ["hh_id", "survey_year"]

# Person-level cleaned pwealth columns (implicate `a`) lagged as predictors.
_LAG_SOURCE_COLUMNS = (
    "property_value_primary_residence_a",
    "financial_assets_value_a",
    "private_insurances_value_a",
    "vehicles_value_a",
    "consumer_debt_value_a",
)

# Household component target (cleaned hwealth, implicate `a`) and its deflation index.
_COMPONENT_COLUMNS: dict[CanonicalComponent, str] = {
    CanonicalComponent.OWNER_OCCUPIED_PROPERTY_GROSS: (
        "hh_net_property_value_primary_residence_a"  # net of mortgage
    ),
    CanonicalComponent.FINANCIAL_ASSETS: "hh_financial_assets_value_a",
    CanonicalComponent.VEHICLES: "hh_vehicles_value_a",
}
_COMPONENT_INDEX: dict[CanonicalComponent, Mapping[int, float] | None] = {
    CanonicalComponent.OWNER_OCCUPIED_PROPERTY_GROSS: None,  # no house-price index yet
    CanonicalComponent.FINANCIAL_ASSETS: MSCI_WORLD_INDEX,
    CanonicalComponent.VEHICLES: None,
}
_OFFICIAL_TOTAL_COLUMN = "hh_net_overall_wealth_including_vehicles_and_student_loans_a"


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
        modules: Cleaned `MODULES` frames; must include `pwealth`, `hwealth`, and the
            feature modules used by `assemble_feature_matrix`.
        n_draws: Number of complete joint draws.
        seed: Seed for model fitting and the draw RNG.
        k: Nearest-donor count for PMM (clipped to each component's owner count).
        level: Central coverage of the reported intervals.

    Returns:
        An `ImputationResult` with the 2022 intervals and a run summary.

    Raises:
        ValueError: If there are no 2022 recipients or no component can be fit.
    """
    heads = _household_heads(modules)
    continuous_columns = [
        spec.column for spec in FEATURE_SPECS if spec.kind == "continuous"
    ] + [f"lagged_{column}" for column in _LAG_SOURCE_COLUMNS]
    categorical_columns = [
        spec.column for spec in FEATURE_SPECS if spec.kind == "categorical"
    ]

    training = heads[heads["survey_year"].isin(_WEALTH_WAVES)]
    recipients = heads[heads["survey_year"] == _PREDICTION_WAVE].reset_index(drop=True)
    if recipients.empty:
        msg = "No 2022 recipients found; cannot impute the prediction wave."
        raise ValueError(msg)

    encoder = fit_categorical_encoder(training, categorical_columns)
    recipient_design = encode_features(
        recipients, continuous_columns=continuous_columns, encoder=encoder
    )
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
            or {int(value > 0.0) for value in values} != {0, 1}
            or owners < _MIN_OWNERS
        ):
            skipped.append(component.value)
            continue
        index = _COMPONENT_INDEX[component]
        if index is not None:
            values = deflate_donor_values(
                values,
                trained["survey_year"].tolist(),
                index_by_year=index,
                target_year=_PREDICTION_WAVE,
            )
        design = encode_features(
            trained, continuous_columns=continuous_columns, encoder=encoder
        )
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


def _household_heads(modules: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    features = assemble_feature_matrix(modules)
    lagged = lagged_wealth(
        modules["pwealth"][["p_id", "survey_year", *_LAG_SOURCE_COLUMNS]],
        value_columns=_LAG_SOURCE_COLUMNS,
    )
    with_lag = features.merge(lagged, on=["p_id", "survey_year"], how="left")
    heads = select_household_heads(with_lag)
    target_columns = [*_COMPONENT_COLUMNS.values(), _OFFICIAL_TOTAL_COLUMN]
    household_wealth = modules["hwealth"][[*_HH_KEYS, *target_columns]]
    return heads.merge(household_wealth, on=_HH_KEYS, how="left")


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
