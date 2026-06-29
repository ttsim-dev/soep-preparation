"""Run the provisional 2022 household-wealth imputation end to end.

This is a **historical-model synthetic projection**, not an edit-and-impute of the 2022
wealth wave: it does not ingest a raw 2022 wealth-answer module, preserves no observed
2022 cell, and predicts every 2022 household from the 2002-2017 completed waves plus
2022 covariates. Where the raw 2022 answers become available they should anchor the
donor pool and be preserved; until then `summary["uses_observed_2022_answers"]` is
`False`.

Targets are bias-free household component totals: the joint components (property,
financial, vehicles) come from the DIW-aggregated `hwealth` file (correctly
share-summed), and the person-direct components (insurances, consumer debt) -- which
have no ownership share and so are absent from the household file -- are summed from
person `pwealth` across members, which is unbiased for fully-represented households (the
partial-unit-nonresponse caveat is in `_household_person_direct`). Property enters gross
with a separate mortgage liability; together with financial, vehicles, pension, and
consumer debt -- plus a residual to the official total -- they reconstruct net wealth.
The pipeline composes the tested blocks:

1. assemble the person feature matrix, attach each person's lagged prior-wave wealth,
   and represent each household by its oldest member (`select_household_heads`);
2. merge the joint household targets and the summed person-direct targets onto heads;
3. restrict training to the wealth waves, fit ownership + amount models per component
   on continuous + one-hot categorical predictors, with cross-wave donor values
   deflated to 2022 terms by asset-class indices (MSCI -> financial, BIS house prices
   -> property, REX bonds -> insurances);
4. simulate joint draws into household net-total bands; within each draw the signed
   reconciliation residual (official total minus the fitted component vector --
   dominated by business / other real estate but also absorbing omitted liabilities,
   editing/measurement discrepancies, and model-definition mismatch) is drawn by PMM
   from observed donor residuals, matched on a signed asinh score, so it preserves sign
   and the empirical distribution and contributes spread to the band rather than a
   deterministic shift. The residual is provisional (a single-wave fit) and flagged as
   a sensitivity term in the summary.

Documented approximations: vehicles, mortgage, and consumer-debt donors are not
deflated (nominal / no asset index); the residual is deflated to 2022 by a
property/equity blend (`RESIDUAL_INDEX`) standing in for its business and
other-real-estate mass; implicate `a` is the representative value; property enters
gross with the mortgage as a separate liability, so net equity is rebuilt per draw.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import numpy as np
import pandas as pd

from soep_preparation.wealth_imputation.components import (
    SECURED_BY,
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
from soep_preparation.wealth_imputation.intervals import (
    distribution_across_draws,
    household_total_intervals,
)
from soep_preparation.wealth_imputation.market_indices import (
    HOUSE_PRICE_INDEX,
    MSCI_WORLD_INDEX,
    RESIDUAL_INDEX,
    REX_BOND_INDEX,
)
from soep_preparation.wealth_imputation.residual_model import ResidualModel
from soep_preparation.wealth_imputation.simulate import (
    ComponentDrawConfig,
    ResidualDrawConfig,
    collect_donor_wave_composition,
    nearest_donor_distances,
    simulate_household_total_draws,
)
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
# share-summed). Property enters gross (appreciated by the house-price index) with the
# mortgage carried as a separate nominal liability, so net equity = gross - mortgage is
# rebuilt inside each draw rather than scaling the mortgage by house prices.
_HW_PROPERTY_GROSS = "hh_property_value_primary_residence_a"
_HW_PROPERTY_NET = "hh_net_property_value_primary_residence_a"
_HW_MORTGAGE = "hh_owner_occupied_mortgage_a"  # derived: gross - net property value
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
    CanonicalComponent.OWNER_OCCUPIED_PROPERTY_GROSS: _HW_PROPERTY_GROSS,
    CanonicalComponent.OWNER_OCCUPIED_MORTGAGE: _HW_MORTGAGE,
    CanonicalComponent.FINANCIAL_ASSETS: _HW_FINANCIAL,
    CanonicalComponent.VEHICLES: _HW_VEHICLES,
    CanonicalComponent.PRIVATE_PENSION: "hh_private_insurances_value_a",
    CanonicalComponent.CONSUMER_DEBT: "hh_consumer_debt_value_a",
}
_COMPONENT_INDEX: dict[CanonicalComponent, Mapping[int, float] | None] = {
    CanonicalComponent.OWNER_OCCUPIED_PROPERTY_GROSS: HOUSE_PRICE_INDEX,
    CanonicalComponent.OWNER_OCCUPIED_MORTGAGE: None,  # nominal debt balance
    CanonicalComponent.FINANCIAL_ASSETS: MSCI_WORLD_INDEX,
    CanonicalComponent.VEHICLES: None,  # depreciating; no asset index
    CanonicalComponent.PRIVATE_PENSION: REX_BOND_INDEX,
    CanonicalComponent.CONSUMER_DEBT: None,  # nominal debt
}
_OFFICIAL_TOTAL_COLUMN = "hh_net_overall_wealth_including_vehicles_and_student_loans_a"

# Inverse of SECURED_BY: the secured liability backed by each asset (property ->
# mortgage). Used to attach each property donor's own mortgage to the property config so
# the coupled draw takes an observed (property, mortgage) pair.
_SECURED_LIABILITY_OF: dict[CanonicalComponent, CanonicalComponent] = {
    asset: liability for liability, asset in SECURED_BY.items()
}


@dataclass(frozen=True)
class ImputationResult:
    """The 2022 household-wealth point estimates with donor-spread bands and a summary.

    Two totals are reported side by side (#66-style honesty about what is validated):

    - `intervals` is the **component-only** total -- the sum of the modelled components.
      It is the primary output because it is what the out-of-fold backtest validates
      (the backtest compares it against the observed completed-component sum).
    - `residual_inclusive_intervals` adds the signed reconciliation residual to the
      official total. It is the more complete economic object but rests on a single-wave
      (2017-only) residual fit that no backtest can validate, so it is a **scenario**,
      not the headline. `None` when no residual model could be fit.

    The `lower`/`upper` bounds are **conditional donor-randomisation spreads**, not
    calibrated predictive intervals: they reflect ownership/PMM draw variability only,
    holding the fitted models and the single wealth implicate fixed -- the residual
    *model* is fixed but its donor value is redrawn per draw -- and carry no modelled
    cross-component covariance. Treat them as a lower bound on predictive uncertainty.
    """

    intervals: pd.DataFrame
    """Component-only total. Columns `hh_id`, `survey_year`, `point_estimate`, `lower`,
    `upper`. This is the primary, backtest-validated output."""
    summary: dict
    """Counts and choices for the run, plus `distribution_across_draws` (the component-
    only predictive distribution across complete draws, not the per-household medians in
    `intervals`) and `residual_inclusive_distribution_across_draws` (the scenario)."""
    residual_inclusive_intervals: pd.DataFrame | None
    """Residual-inclusive scenario total, same columns as `intervals`; `None` when no
    residual model was fit."""
    component_only_draws: pd.DataFrame | None
    """The component-only draw table (`hh_id`, `survey_year`, `household_total_draw`),
    one row per household per draw. Populated only when `run_imputation` is called with
    `keep_draws=True` (the backtest needs it for a draw-level distribution); else
    `None`."""


def run_imputation(  # noqa: PLR0913 -- keyword-only run settings + backtest waves
    modules: Mapping[str, pd.DataFrame],
    *,
    n_draws: int,
    seed: int,
    k: int,
    level: float = 0.9,
    prediction_wave: int = _PREDICTION_WAVE,
    training_waves: Sequence[int] = _WEALTH_WAVES,
    keep_draws: bool = False,
    caliper: float | None = None,
) -> ImputationResult:
    """Impute household net wealth for `prediction_wave` as point estimates with bands.

    Args:
        modules: Cleaned `MODULES` frames; must include `pwealth`, `hwealth`, and the
            feature modules used by `assemble_feature_matrix`.
        n_draws: Number of complete joint draws.
        seed: Seed for model fitting and the draw RNG.
        k: Nearest-donor count for PMM (clipped to each component's owner count).
        level: Central level of the reported donor-spread bands (not calibrated
            coverage).
        prediction_wave: The survey year to impute (2022 in production; an earlier wave
            for an out-of-fold backtest).
        training_waves: The wealth waves to fit on (the prediction wave is always
            excluded so a backtest stays out of fold).
        keep_draws: If `True`, attach the component-only draw table to the result as
            `component_only_draws` (the backtest needs it for a draw-level summary).
        caliper: Optional maximum donor score distance per component draw. `None` (the
            default) is diagnostics-only: the point estimate is unchanged, and the
            `out_of_support` share is reported as `None` because there is no threshold.
            When set, it becomes the support gate -- out-of-caliper recipients are
            *flagged* in `out_of_support`, never dropped, so the household count and the
            draw set are unchanged (dropping would bias the distribution). A recipient
            with no donor within the caliper raises. This gates extrapolation; it does
            not create target-wave information.

    Returns:
        An `ImputationResult` with the prediction-wave intervals and a run summary.

    Raises:
        ValueError: If there are no recipients in the prediction wave, no component can
            be fit, or a recipient has no donor within `caliper`.
    """
    heads = _household_heads(modules)
    continuous_columns = [
        spec.column for spec in FEATURE_SPECS if spec.kind == "continuous"
    ] + [f"lagged_{column}" for column in _LAG_SOURCE_COLUMNS]
    categorical_columns = [
        spec.column for spec in FEATURE_SPECS if spec.kind == "categorical"
    ]

    in_training_waves = heads["survey_year"].isin(training_waves)
    training = heads[in_training_waves & (heads["survey_year"] != prediction_wave)]
    recipients = heads[heads["survey_year"] == prediction_wave].reset_index(drop=True)
    if recipients.empty:
        msg = f"No recipients in prediction wave {prediction_wave}."
        raise ValueError(msg)

    encoder = fit_categorical_encoder(
        training, categorical_columns, continuous_columns=continuous_columns
    )
    recipient_design = encode_features(
        recipients, continuous_columns=continuous_columns, encoder=encoder
    )
    recipient_keys = recipients[_KEYS]

    configs = []
    used: list[CanonicalComponent] = []
    skipped: list[str] = []
    mortgage_donor_pool_size: int | None = None
    for component, column in _COMPONENT_COLUMNS.items():
        # Secured liabilities (the mortgage) are fit on the donors who own the backing
        # asset, so the coupled draw never inherits a mortgage from a non-owner.
        trained = _secured_training_subset(training.dropna(subset=[column]), component)
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
                target_year=prediction_wave,
            )
        design = encode_features(
            trained, continuous_columns=continuous_columns, encoder=encoder
        )
        models = fit_component_models(design, values, seed=seed)
        owner_mask = values > 0.0
        if component is CanonicalComponent.OWNER_OCCUPIED_MORTGAGE:
            mortgage_donor_pool_size = int(owner_mask.sum())
        # A backing asset (owner-occupied property) carries each donor's own secured
        # liability (the nominal mortgage) so the coupled draw takes an observed
        # (property, mortgage) pair rather than recombining a low property with an
        # unrelated high mortgage. A donor whose mortgage is unobserved cannot form a
        # coherent pair and is dropped from the asset's donor pool.
        liability = _SECURED_LIABILITY_OF.get(component)
        if liability is not None:
            paired_liability = pd.to_numeric(
                trained[_COMPONENT_COLUMNS[liability]], errors="coerce"
            ).to_numpy(dtype="float64")
            donor_keep = owner_mask & np.isfinite(paired_liability)
            paired_liability_values = paired_liability[donor_keep]
        else:
            donor_keep = owner_mask
            paired_liability_values = None
        donor_year = trained["survey_year"].to_numpy()[donor_keep]
        configs.append(
            build_component_config(
                component=component,
                models=models,
                recipient_features=recipient_design,
                recipient_shares=np.ones(len(recipient_keys), dtype="float64"),
                donor_features=design[donor_keep],
                donor_values=values[donor_keep],
                k=min(k, int(donor_keep.sum())),
                donor_year=donor_year,
                paired_liability_values=paired_liability_values,
            )
        )
        used.append(component)
    if not configs:
        msg = "No wealth component could be fit on the historical waves."
        raise ValueError(msg)

    config_by_component = dict(zip(used, configs, strict=True))
    mortgage_config = config_by_component.get(
        CanonicalComponent.OWNER_OCCUPIED_MORTGAGE
    )
    property_config = config_by_component.get(
        CanonicalComponent.OWNER_OCCUPIED_PROPERTY_GROSS
    )
    mortgage_without_property_share = (
        _mortgage_without_property_share(
            mortgage_config.ownership_prob, property_config.ownership_prob
        )
        if mortgage_config is not None and property_config is not None
        else None
    )

    have_total, residual = _training_residual(
        training, used, target_year=prediction_wave
    )
    residual_config = None
    residual_model_kind = "none"
    donor_pool_mean_residual = 0.0
    if len(have_total) >= _MIN_TRAINING_ROWS:
        residual_design = encode_features(
            have_total, continuous_columns=continuous_columns, encoder=encoder
        )
        model = ResidualModel.fit(residual_design, residual)
        residual_config = ResidualDrawConfig(
            recipient_predicted=model.predict(recipient_design),
            donor_predicted=model.predict(residual_design),
            donor_observed=residual,
            k=min(k, residual.size),
        )
        residual_model_kind = "signed_pmm"
        # Unweighted mean of the residual donor pool. PMM reweights donors by recipient
        # score, so the mean residual actually drawn across recipients differs from this
        # pool mean; this is a donor-pool diagnostic, not the realised contribution.
        donor_pool_mean_residual = float(np.mean(residual))

    draws = simulate_household_total_draws(
        recipient_keys,
        configs,
        n_draws=n_draws,
        rng=np.random.default_rng(seed),
        residual_config=residual_config,
        caliper=caliper,
    )
    out_of_support = _out_of_support_summary(configs, caliper=caliper)
    # A separate RNG keeps the main draws byte-for-byte identical: wave attribution is a
    # diagnostic and must not perturb the point-estimate draw stream.
    donor_wave_composition = collect_donor_wave_composition(
        recipient_keys,
        configs,
        n_draws=n_draws,
        rng=np.random.default_rng(seed),
        caliper=caliper,
    )
    # Component-only is the primary output (the backtest validates it). The residual-
    # inclusive scenario lives on the same draws (`residual_inclusive_total_draw`), so
    # both totals are household-consistent.
    intervals = household_total_intervals(draws, level=level)
    residual_inclusive_intervals = None
    residual_inclusive_distribution = None
    if "residual_inclusive_total_draw" in draws.columns:
        scenario_draws = draws[["hh_id", "survey_year"]].assign(
            household_total_draw=draws["residual_inclusive_total_draw"]
        )
        residual_inclusive_intervals = household_total_intervals(
            scenario_draws, level=level
        )
        residual_inclusive_distribution = distribution_across_draws(
            scenario_draws, level=level
        )
    summary = {
        "n_recipients": len(recipient_keys),
        "n_training_heads": len(training),
        "components_used": [component.value for component in used],
        "components_skipped": skipped,
        "primary_total": "component_only",
        "residual_model": residual_model_kind,
        "residual_is_sensitivity": True,
        "residual_validated_out_of_sample": False,
        "uses_observed_2022_answers": False,
        "uses_support_gate": caliper is not None,
        "donor_pool_mean_residual": donor_pool_mean_residual,
        # Support transparency (F4): per-component nearest-donor score-distance
        # quantiles and, when a caliper gates the run, the share of recipients whose
        # nearest donor exceeds it. This quantifies the extrapolation the projection
        # cannot avoid while the target wave has no anchoring wealth answers; it does
        # not create that information. `out_of_support_share` is `None` with no caliper.
        "out_of_support": out_of_support,
        # Share of each component's drawn donors sourced from each historical wave, so
        # the waves the target-wave draws lean on are visible.
        "donor_wave_composition": donor_wave_composition,
        # Coherence diagnostic (F2): the expected share of recipients drawn as a
        # mortgage holder but a non-property-owner -- an incoherent balance sheet no
        # donor household has. The coupled property/mortgage draw zeros the mortgage for
        # every non-property-owner, so this is zero by construction whenever both the
        # property and mortgage components are fit, and `None` when either is skipped.
        "mortgage_without_property_expected_share": mortgage_without_property_share,
        # Number of property-owning mortgage donors backing the coupled mortgage amount
        # draw; `None` when the mortgage component is skipped.
        "mortgage_donor_pool_size": mortgage_donor_pool_size,
        "n_draws": n_draws,
        "k": k,
        "level": level,
        # Predictive distribution computed WITHIN each complete draw then summarised
        # across draws -- the per-household medians in `intervals` are not this. The
        # primary distribution is component-only; the residual scenario is reported
        # separately (None when no residual model was fit).
        "distribution_across_draws": distribution_across_draws(draws, level=level),
        "residual_inclusive_distribution_across_draws": residual_inclusive_distribution,
    }
    component_only_draws = (
        draws[["hh_id", "survey_year", "household_total_draw"]] if keep_draws else None
    )
    return ImputationResult(
        intervals=intervals,
        summary=summary,
        residual_inclusive_intervals=residual_inclusive_intervals,
        component_only_draws=component_only_draws,
    )


def _out_of_support_summary(
    configs: Sequence[ComponentDrawConfig],
    *,
    caliper: float | None,
) -> dict[str, dict[str, float | None]]:
    """Summarise each component's nearest-donor distances and out-of-support share.

    For every component the recipients' nearest-donor score distances
    (`nearest_donor_distances`) are reduced to their median, p90, and p99. When a
    `caliper` gates the run, `out_of_support_share` is the share of recipients whose
    nearest donor exceeds it -- the recipients flagged as extrapolations. Without a
    caliper there is no threshold, so `out_of_support_share` is `None` while the
    distance quantiles are still reported.
    """
    distances_by_component = nearest_donor_distances(configs)
    summary: dict[str, dict[str, float | None]] = {}
    for component, distances in distances_by_component.items():
        share: float | None = (
            float(np.mean(distances > caliper)) if caliper is not None else None
        )
        summary[component] = {
            "median": float(np.quantile(distances, 0.5)),
            "p90": float(np.quantile(distances, 0.9)),
            "p99": float(np.quantile(distances, 0.99)),
            "out_of_support_share": share,
        }
    return summary


def _mortgage_without_property_share(
    mortgage_ownership_prob: np.ndarray,
    property_ownership_prob: np.ndarray,
) -> float:
    """Return the expected (mortgage, no property) incidence share, structurally zero.

    The coupled property/mortgage draw (`simulate._draw_secured_housing`) zeros the
    mortgage -- ownership and amount -- for every recipient drawn as a non-property-
    owner, so a recipient can never hold an owner-occupied mortgage without the owner-
    occupied property that secures it. The share is therefore exactly zero whatever the
    per-recipient ownership probabilities; the probability arrays are accepted only to
    document that this coherence holds for the components actually drawn.

    Args:
        mortgage_ownership_prob: Per-recipient mortgage incidence probability.
        property_ownership_prob: Per-recipient property incidence probability.

    Returns:
        `0.0`.
    """
    del mortgage_ownership_prob, property_ownership_prob
    return 0.0


def _secured_training_subset(
    trained: pd.DataFrame, component: CanonicalComponent
) -> pd.DataFrame:
    """Restrict a secured liability's donor rows to owners of its backing asset.

    A secured liability (the owner-occupied mortgage) is drawn only for owners of its
    backing asset, so its donor pool must be the rows that own that asset (gross backing
    value > 0). Unsecured components pass through unchanged.

    Args:
        trained: Donor rows already restricted to non-missing values of `component`.
        component: The component whose donor pool is being built.

    Returns:
        `trained` filtered to backing-asset owners for a secured liability, else
        `trained` unchanged.
    """
    backing = SECURED_BY.get(component)
    if backing is None:
        return trained
    backing_value = pd.to_numeric(trained[_COMPONENT_COLUMNS[backing]], errors="coerce")
    return trained[backing_value > 0.0]


def observed_component_total(
    modules: Mapping[str, pd.DataFrame], wave: int
) -> pd.DataFrame:
    """Return the signed completed-component sum for fully observed households.

    This is the comparison target for the out-of-fold backtest: the same six components
    the imputation models (property gross, mortgage, financial, vehicles, pension,
    consumer debt), signed and summed per household from the actual `wave` data, with no
    residual. Only households whose **every** modelled component is non-missing enter
    the roster -- a household missing any component has no comparable observed total,
    since zero-filling the missing cells would turn a partial vector into a fake
    observed sum (deflating its level and inflating both the zero mass and the household
    count of the backtest truth).

    The kept quantity is the *completed-component* sum: the signed total over the six
    modelled components for households that observed all of them. It is not the
    household's official net total and carries no residual.

    Args:
        modules: Cleaned `MODULES` frames.
        wave: The survey year whose completed-component sum to return.

    Returns:
        Columns `hh_id`, `survey_year`, `observed_total` (float64); one row per fully
        observed household. The fraction of `wave` households dropped for an incomplete
        component vector is the support loss this restriction costs.
    """
    heads = _household_heads(modules)
    wave_heads = heads[heads["survey_year"] == wave]
    component_columns = list(_COMPONENT_COLUMNS.values())
    numeric = wave_heads[component_columns].apply(
        lambda column: pd.to_numeric(column, errors="coerce")
    )
    fully_observed = numeric.notna().all(axis=1).to_numpy()
    observed = wave_heads.loc[fully_observed].reset_index(drop=True)
    numeric = numeric.loc[fully_observed].reset_index(drop=True)
    total = np.zeros(len(observed), dtype="float64")
    for component, column in _COMPONENT_COLUMNS.items():
        values = numeric[column].to_numpy(dtype="float64")
        total += component_sign(component) * values
    return observed[_HH_KEYS].assign(observed_total=total)


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
        [
            *_HH_KEYS,
            _HW_PROPERTY_GROSS,
            _HW_PROPERTY_NET,
            _HW_FINANCIAL,
            _HW_VEHICLES,
            _OFFICIAL_TOTAL_COLUMN,
        ]
    ].copy()
    gross = pd.to_numeric(household_wealth[_HW_PROPERTY_GROSS], errors="coerce")
    net = pd.to_numeric(household_wealth[_HW_PROPERTY_NET], errors="coerce")
    household_wealth[_HW_MORTGAGE] = (gross - net).clip(lower=0.0)
    heads = heads.merge(household_wealth, on=_HH_KEYS, how="left")
    return heads.merge(_household_person_direct(pwealth), on=_HH_KEYS, how="left")


def _household_person_direct(pwealth: pd.DataFrame) -> pd.DataFrame:
    """Sum person-direct components (insurances, consumer debt) to the household.

    These have no ownership share, so a plain member sum avoids the joint-ownership
    double-counting that plagues the joint components. The sum uses `min_count=1`, so a
    household whose members are *all* missing a component yields `NA`, not `0` -- pandas
    sums an all-missing group to zero, which would confound nonresponse with a genuine
    structural zero and feed a spurious observed zero into the truth roster.

    Even with `min_count=1` the member sum is unbiased **only for households whose
    eligible adults are all represented** in person `pwealth`: under partial unit
    nonresponse (an adult lacking an individual wealth response, a PUNR adult absent
    from `pwealth` altogether) the member sum omits that person's holdings, which then
    surface
    unpredictably in the residual. A proper fix needs an eligible-adult roster and
    component-level PUNR completion.
    """
    # REVIEW (sample-frame / PUNR): the member sum omits unrepresented eligible adults,
    # so person-direct totals are biased under partial unit nonresponse. A faithful fix
    # needs an eligible-adult roster and component-level PUNR completion; left for human
    # decision.
    source_to_target = {
        source: target for target, source in _PERSON_DIRECT_SOURCE.items()
    }
    numeric = pwealth[list(source_to_target)].apply(
        lambda column: pd.to_numeric(column, errors="coerce")
    )
    keyed = pd.concat([pwealth[_HH_KEYS], numeric], axis=1)
    grouped = keyed.groupby(_HH_KEYS, as_index=False).sum(min_count=1)
    return grouped.rename(columns=source_to_target)


def _training_residual(
    training: pd.DataFrame,
    used: Sequence[CanonicalComponent],
    *,
    target_year: int,
    subset: np.ndarray | None = None,
) -> tuple[pd.DataFrame, np.ndarray]:
    """Return training heads with an official total and their residual, in 2022 terms.

    The residual is `official_net_total - sum(component_sign * modelled_component)`
    over the fitted components, kept in euros and signed (negative when modelled wealth
    exceeds the official total). This reconciliation residual is dominated by the
    unmodelled business and other-real-estate mass but also absorbs omitted
    liabilities, editing/measurement discrepancies, and model-definition mismatch. It
    is deflated from each household's wave into `target_year` terms by `RESIDUAL_INDEX`
    -- a property/equity blend keyed to that dominant mass -- so the time component is
    explicit rather than absorbed into the OLS coefficients.

    Only rows with an official total *and* a complete used-component vector enter the
    residual donor pool: a missing modelled component is dropped, not zero-filled, since
    zero-filling would push that component's full mass into the residual outcome and
    bias the donor pool. The fraction of `official-total` rows dropped for an incomplete
    component vector is the support loss this restriction costs. Rows whose residual is
    non-finite are also dropped so the frame and the residual array stay aligned for
    `ResidualModel.fit`.

    Args:
        training: The training heads frame.
        used: The fitted components entering the residual.
        target_year: The wave whose price level the residual is deflated into.
        subset: Optional boolean mask over `training`'s rows; when given, only the
            selected rows enter the residual. Used by the 2017 residual cross-fit to
            restrict the donor pool to a single wave. `None` keeps every row.

    Returns:
        The residual-eligible heads frame and their signed, deflated residual array.
    """
    if subset is not None:
        training = training[np.asarray(subset, dtype=bool)]
    used_columns = [_COMPONENT_COLUMNS[component] for component in used]
    have_total = training.dropna(subset=[_OFFICIAL_TOTAL_COLUMN, *used_columns])
    if have_total.empty:
        return have_total, np.empty(0, dtype="float64")
    modelled = np.zeros(len(have_total), dtype="float64")
    for component in used:
        values = pd.to_numeric(
            have_total[_COMPONENT_COLUMNS[component]], errors="coerce"
        ).to_numpy(dtype="float64")
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


def _fittable_components(training: pd.DataFrame) -> list[CanonicalComponent]:
    """Return the components that pass the production fit gate on `training`.

    A component is kept when its training rows reach `_MIN_TRAINING_ROWS`, both
    ownership classes are present, and at least `_MIN_OWNERS` rows own it. This mirrors
    the gate in `run_imputation`, so the residual is defined over the same components
    the production run models.
    """
    fittable: list[CanonicalComponent] = []
    for component, column in _COMPONENT_COLUMNS.items():
        trained = training.dropna(subset=[column])
        values = trained[column].to_numpy(dtype="float64")
        owners = int((values > 0.0).sum())
        if (
            len(trained) < _MIN_TRAINING_ROWS
            or {int(value > 0.0) for value in values} != {0, 1}
            or owners < _MIN_OWNERS
        ):
            continue
        fittable.append(component)
    return fittable


def residual_cross_fit_inputs(
    modules: Mapping[str, pd.DataFrame],
    *,
    wave: int = 2017,
    target_year: int = _PREDICTION_WAVE,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Build the residual-eligible design, residual, and component total for one wave.

    Assembles the household heads, restricts to the residual-eligible households in
    `wave` (an official total and a complete modelled-component vector), encodes their
    features, and returns the arrays the 2017 residual cross-fit consumes. The component
    total and the residual are both brought into `target_year` price terms by
    `RESIDUAL_INDEX`, so `component_total + residual` reconstructs the deflated official
    total; within a single wave the deflation is one constant factor, so the two stay on
    the same price basis.

    Only the wave with the augmented official total `n011h` (2017) carries a residual,
    so this validation is cross-sectional within that wave; it cannot test the
    2017->2022 temporal transport the production residual scenario relies on.

    Args:
        modules: Cleaned `MODULES` frames.
        wave: The residual-eligible wave (2017 in production).
        target_year: The price level the residual and component total are deflated into.

    Returns:
        The encoded design matrix, the signed deflated residual, and the deflated
        modelled component total, one row per residual-eligible household in `wave`.

    Raises:
        ValueError: If no component passes the fit gate or no residual-eligible
            household exists in `wave`.
    """
    heads = _household_heads(modules)
    continuous_columns = [
        spec.column for spec in FEATURE_SPECS if spec.kind == "continuous"
    ] + [f"lagged_{column}" for column in _LAG_SOURCE_COLUMNS]
    categorical_columns = [
        spec.column for spec in FEATURE_SPECS if spec.kind == "categorical"
    ]
    wave_heads = heads[heads["survey_year"] == wave].reset_index(drop=True)
    used = _fittable_components(wave_heads)
    if not used:
        msg = f"No wealth component could be fit on wave {wave}."
        raise ValueError(msg)
    eligible, residual = _training_residual(wave_heads, used, target_year=target_year)
    if eligible.empty:
        msg = f"No residual-eligible household in wave {wave}."
        raise ValueError(msg)
    encoder = fit_categorical_encoder(
        eligible, categorical_columns, continuous_columns=continuous_columns
    )
    design = encode_features(
        eligible, continuous_columns=continuous_columns, encoder=encoder
    )
    modelled = np.zeros(len(eligible), dtype="float64")
    for component in used:
        values = pd.to_numeric(
            eligible[_COMPONENT_COLUMNS[component]], errors="coerce"
        ).to_numpy(dtype="float64")
        modelled += component_sign(component) * values
    component_total = deflate_donor_values(
        modelled,
        eligible["survey_year"].tolist(),
        index_by_year=RESIDUAL_INDEX,
        target_year=target_year,
    )
    return design, residual, component_total
