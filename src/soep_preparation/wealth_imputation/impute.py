"""Run the provisional 2022 household-wealth imputation end to end.

Targets are bias-free household component totals: the joint components (property,
financial, vehicles) come from the DIW-aggregated `hwealth` file (correctly
share-summed), and the person-direct components (insurances, consumer debt) -- which
have no ownership share and so are absent from the household file -- are summed from
person `pwealth` across members, which is unbiased. The five components plus a residual
to the official total reconstruct net wealth. The pipeline composes the tested blocks:

1. assemble the person feature matrix, attach each person's lagged prior-wave wealth,
   and represent each household by its oldest member (`select_household_heads`);
2. merge the joint household targets and the summed person-direct targets onto heads;
3. restrict training to the wealth waves, fit ownership + amount models per component
   on continuous + one-hot categorical predictors, with cross-wave donor values
   deflated to 2022 terms by asset-class indices (MSCI -> financial, BIS house prices
   -> property, REX bonds -> insurances);
4. simulate joint draws into household net-total intervals, shifted by a per-household
   residual to the official total -- an OLS model on the design allocates the
   unmodelled business / other-real-estate mass to the households whose covariates
   predict it, rather than spreading the population mean uniformly.

Documented approximations: vehicles and consumer-debt donors are not deflated (no
asset index); the residual is deflated to 2022 by a property/equity blend
(`RESIDUAL_INDEX`) standing in for its business and other-real-estate mass; implicate
`a` is the representative value; household property is taken net of mortgage.
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
from soep_preparation.wealth_imputation.market_indices import (
    HOUSE_PRICE_INDEX,
    MSCI_WORLD_INDEX,
    RESIDUAL_INDEX,
    REX_BOND_INDEX,
)
from soep_preparation.wealth_imputation.residual_model import ResidualModel
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

# Joint components taken from the DIW-aggregated household file (correctly
# share-summed). Net property folds in the mortgage.
_HW_PROPERTY = "hh_net_property_value_primary_residence_a"
_HW_FINANCIAL = "hh_financial_assets_value_a"
_HW_VEHICLES = "hh_vehicles_value_a"

# Person-direct components (no ownership share) absent from the household file: summed
# from person `pwealth` across members, which is unbiased because they are individual
# holdings (no joint double-counting).
_PERSON_DIRECT_SOURCE = {
    "hh_private_insurances_value_a": "private_insurances_value_a",
    "hh_consumer_debt_value_a": "consumer_debt_value_a",
}

# Each component's target column and the asset-class index used to deflate its donors.
_COMPONENT_COLUMNS: dict[CanonicalComponent, str] = {
    CanonicalComponent.OWNER_OCCUPIED_PROPERTY_GROSS: _HW_PROPERTY,
    CanonicalComponent.FINANCIAL_ASSETS: _HW_FINANCIAL,
    CanonicalComponent.VEHICLES: _HW_VEHICLES,
    CanonicalComponent.PRIVATE_PENSION: "hh_private_insurances_value_a",
    CanonicalComponent.CONSUMER_DEBT: "hh_consumer_debt_value_a",
}
_COMPONENT_INDEX: dict[CanonicalComponent, Mapping[int, float] | None] = {
    CanonicalComponent.OWNER_OCCUPIED_PROPERTY_GROSS: HOUSE_PRICE_INDEX,
    CanonicalComponent.FINANCIAL_ASSETS: MSCI_WORLD_INDEX,
    CanonicalComponent.VEHICLES: None,  # depreciating; no asset index
    CanonicalComponent.PRIVATE_PENSION: REX_BOND_INDEX,
    CanonicalComponent.CONSUMER_DEBT: None,  # nominal debt
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
    have_total, residual = _training_residual(
        training, used, target_year=_PREDICTION_WAVE
    )
    if len(have_total) >= _MIN_TRAINING_ROWS:
        residual_design = encode_features(
            have_total, continuous_columns=continuous_columns, encoder=encoder
        )
        recipient_residual = ResidualModel.fit(residual_design, residual).predict(
            recipient_design
        )
        residual_model_kind = "ols"
    else:
        flat = float(np.mean(residual)) if residual.size else 0.0
        recipient_residual = np.full(len(recipients), flat, dtype="float64")
        residual_model_kind = "flat"
    residual_frame = recipients[_HH_KEYS].assign(applied_residual=recipient_residual)
    intervals = intervals.merge(
        residual_frame, on=_HH_KEYS, how="left", validate="one_to_one"
    )
    for column in ("point_estimate", "lower", "upper"):
        intervals[column] = intervals[column] + intervals["applied_residual"]
    intervals = intervals.drop(columns="applied_residual")
    summary = {
        "n_recipients": len(recipient_keys),
        "n_training_heads": len(training),
        "components_used": [component.value for component in used],
        "components_skipped": skipped,
        "residual_model": residual_model_kind,
        "mean_residual": float(np.mean(recipient_residual)),
        "n_draws": n_draws,
        "k": k,
        "level": level,
    }
    return ImputationResult(intervals=intervals, summary=summary)


def _household_heads(modules: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    pwealth = modules["pwealth"]
    features = assemble_feature_matrix(modules)
    lagged = lagged_wealth(
        pwealth[["p_id", "survey_year", *_LAG_SOURCE_COLUMNS]],
        value_columns=_LAG_SOURCE_COLUMNS,
    )
    with_lag = features.merge(lagged, on=["p_id", "survey_year"], how="left")
    heads = select_household_heads(with_lag)
    household_wealth = modules["hwealth"][
        [*_HH_KEYS, _HW_PROPERTY, _HW_FINANCIAL, _HW_VEHICLES, _OFFICIAL_TOTAL_COLUMN]
    ]
    heads = heads.merge(household_wealth, on=_HH_KEYS, how="left")
    return heads.merge(_household_person_direct(pwealth), on=_HH_KEYS, how="left")


def _household_person_direct(pwealth: pd.DataFrame) -> pd.DataFrame:
    """Sum person-direct components (insurances, consumer debt) to the household.

    These have no ownership share, so a plain member sum is the correct household
    total -- there is no joint-ownership double-counting to worry about.
    """
    aggregation = {
        target: (source, "sum") for target, source in _PERSON_DIRECT_SOURCE.items()
    }
    return pwealth.groupby(_HH_KEYS, as_index=False).agg(**aggregation)


def _training_residual(
    training: pd.DataFrame,
    used: Sequence[CanonicalComponent],
    *,
    target_year: int,
) -> tuple[pd.DataFrame, np.ndarray]:
    """Return training heads with an official total and their residual, in 2022 terms.

    The residual is `official_net_total - sum(component_sign * modelled_component)`
    over the fitted components, kept in euros and signed (negative when modelled wealth
    exceeds the official total). It is deflated from each household's wave into
    `target_year` terms by `RESIDUAL_INDEX` -- a property/equity blend standing in for
    the unmodelled business and other-real-estate mass -- so the time component is
    explicit rather than absorbed into the OLS coefficients. Rows whose residual is
    non-finite are dropped so the frame and the residual array stay aligned for
    `ResidualModel.fit`.
    """
    have_total = training.dropna(subset=[_OFFICIAL_TOTAL_COLUMN])
    if have_total.empty:
        return have_total, np.empty(0, dtype="float64")
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
    finite = np.isfinite(residual)
    have_total = have_total[finite]
    deflated = deflate_donor_values(
        residual[finite],
        have_total["survey_year"].tolist(),
        index_by_year=RESIDUAL_INDEX,
        target_year=target_year,
    )
    return have_total, deflated
