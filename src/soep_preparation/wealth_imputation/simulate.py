"""Joint-draw simulation of household wealth totals with donor-spread bands.

Each draw runs every component's sequential `draw_component`, sums the signed
person-component values to a household net total, and the spread of those totals
across draws becomes the band. Summing within each draw before taking the spread is
the correct way to aggregate -- component band endpoints are never summed.

The total is a **largely independent conditional recombination (sensitivity)**, not a
fully coherent joint draw. The owner-occupied property and its mortgage are the one
exception: they are drawn as a single donor bundle (`_draw_secured_housing`), so net
home equity is always an observed pair's. Every other component and the accounting
residual are still drawn independently given the covariates, so for those the only
cross-component dependence carried into the total is through shared covariates -- the
empirical co-movement of the unsecured components within a real household is not
reproduced, and the household accounting law that ties the components to the official
total is not enforced draw by draw. The resulting bands are therefore **conditional
donor-randomisation spreads**, not calibrated predictive intervals: they hold the fitted
models and the chosen implicate fixed (the residual *model* is fixed but its donor value
is redrawn each draw). One shared RNG threads all draws so a fixed seed reproduces the
whole simulation.
"""

# REVIEW (F4): apart from the property/mortgage bundle (`_draw_secured_housing`), the
# unsecured components and the accounting residual are still drawn independently, so the
# household accounting law and the empirical co-movement of the unsecured components
# are not preserved. Extending the bundle to all components of one observed household is
# left for human decision.

from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
import pandas as pd

from soep_preparation.wealth_imputation.aggregate import household_net_total
from soep_preparation.wealth_imputation.components import (
    SECURED_BY,
    CanonicalComponent,
)
from soep_preparation.wealth_imputation.donors import pmm_draw
from soep_preparation.wealth_imputation.intervals import household_total_intervals
from soep_preparation.wealth_imputation.shares import resolve_person_amount
from soep_preparation.wealth_imputation.value_generator import (
    ComponentDraw,
    draw_component,
)

_RECIPIENT_KEYS = ("p_id", "hh_id", "survey_year")


@dataclass(frozen=True)
class ResidualDrawConfig:
    """Inputs for drawing the signed unmodelled-components residual per recipient.

    The residual is drawn by predictive mean matching on the signed matching score, so
    it preserves the empirical sign and distribution of the donor residuals and adds
    genuine spread to the household total across draws.
    """

    recipient_predicted: np.ndarray
    """Predicted signed residual (matching score) per recipient."""
    donor_predicted: np.ndarray
    """Predicted signed residual (matching score) per donor."""
    donor_observed: np.ndarray
    """Observed signed (deflated) residual per donor."""
    k: int
    """Number of nearest eligible donors to sample from (>= 1)."""


@dataclass(frozen=True)
class ComponentDrawConfig:
    """Per-component inputs for one component's draw over the recipients."""

    component: CanonicalComponent
    """Canonical component drawn by this config."""
    ownership_prob: np.ndarray
    """Ownership incidence probability per recipient, in `[0, 1]`."""
    ownership_share: np.ndarray
    """Ownership share per recipient, in `[0, 1]` for owners."""
    recipient_predicted: np.ndarray
    """Recipient matching score on the asinh axis (`AmountModel.predict_score`)."""
    donor_predicted: np.ndarray
    """Donor matching scores on the asinh axis."""
    donor_observed: np.ndarray
    """Observed gross euro amounts for donors."""
    scale: float
    """Positive, finite component scale for the asinh transform."""
    k: int
    """Number of nearest eligible donors to sample from (>= 1)."""
    donor_year: np.ndarray | None = None
    """Survey year of each donor, aligned to `donor_observed`. Set for donor-wave
    composition diagnostics; `None` when wave attribution is not needed."""
    paired_liability_observed: np.ndarray | None = None
    """Secured liability amount of each donor, aligned to `donor_observed`. Set on a
    backing-asset config (owner-occupied property) so its secured liability (the
    mortgage) can be drawn from the *same* donor household via `_draw_secured_housing`,
    keeping each drawn (asset, liability) pair an observed one. `None` on every
    unsecured component and on the liability's own config."""


def simulate_household_total_draws(  # noqa: PLR0913 -- keyword-only simulation knobs
    recipients: pd.DataFrame,
    configs: Sequence[ComponentDrawConfig],
    *,
    n_draws: int,
    rng: np.random.Generator,
    residual_config: ResidualDrawConfig | None = None,
    caliper: float | None = None,
) -> pd.DataFrame:
    """Simulate the complete joint draws of household net-wealth totals.

    Args:
        recipients: Columns `p_id`, `hh_id`, `survey_year`; one row per recipient,
            aligned to every config's per-recipient arrays.
        configs: One `ComponentDrawConfig` per modelled component.
        n_draws: Number of complete joint draws (>= 1).
        rng: NumPy random generator threaded through all draws.
        residual_config: If given, the signed reconciliation residual is drawn by PMM
            each draw and reported as a separate `residual_inclusive_total_draw` column;
            the primary `household_total_draw` stays component-only.
        caliper: Optional maximum donor score distance per component draw. `None` (the
            default) draws the nearest donor regardless of distance, so the point
            estimate is unchanged. When set, out-of-caliper recipients are flagged in
            the support diagnostics rather than dropped, so the draw set and household
            count are unchanged. A recipient with no donor within the caliper raises.

    Returns:
        Columns `hh_id`, `survey_year`, `household_total_draw` (float64), the
        component-only total; `n_draws` rows per household, one per complete draw. When
        `residual_config` is given, an extra `residual_inclusive_total_draw` column adds
        the per-draw signed residual to the component-only total (the same draw, so the
        two totals are household-consistent). Draw membership is the within-household
        row order, as `distribution_across_draws` expects.

    Raises:
        ValueError: On missing recipient keys, no configs, `n_draws < 1`, a config
            whose arrays do not align with the recipients, or a recipient with no donor
            within `caliper`.
    """
    _fail_if_simulation_inputs_invalid(recipients, configs, n_draws)
    keys = recipients[list(_RECIPIENT_KEYS)]
    draw_frames = []
    for _ in range(n_draws):
        drawn_by_component = _draw_all_components(configs, rng, caliper=caliper)
        component_rows = [
            keys[["hh_id", "survey_year"]].assign(
                component=component.value,
                household_component_value=drawn.person_value,
            )
            for component, drawn in drawn_by_component.items()
        ]
        per_draw = pd.concat(component_rows, ignore_index=True)
        totals = household_net_total(per_draw).rename(
            columns={"net_total": "household_total_draw"}
        )
        if residual_config is not None:
            residual_result = pmm_draw(
                residual_config.recipient_predicted,
                residual_config.donor_predicted,
                residual_config.donor_observed,
                residual_config.k,
                rng,
            )
            drawn_residual = residual_result.values  # noqa: PD011 -- PmmResult, not Series
            residual_frame = keys[["hh_id", "survey_year"]].assign(
                residual_draw=drawn_residual
            )
            totals = totals.merge(
                residual_frame, on=["hh_id", "survey_year"], how="left"
            )
            # `household_total_draw` stays component-only (the primary,
            # backtest-validated total). The residual-inclusive total is a separate
            # scenario column on the same draw, so the two stay household-consistent.
            totals["residual_inclusive_total_draw"] = (
                totals["household_total_draw"] + totals["residual_draw"]
            )
            totals = totals.drop(columns="residual_draw")
        draw_frames.append(totals)
    return pd.concat(draw_frames, ignore_index=True)


def simulate_household_totals(  # noqa: PLR0913 -- keyword-only simulation knobs
    recipients: pd.DataFrame,
    configs: Sequence[ComponentDrawConfig],
    *,
    n_draws: int,
    rng: np.random.Generator,
    level: float = 0.9,
    residual_config: ResidualDrawConfig | None = None,
) -> pd.DataFrame:
    """Simulate household net-wealth totals and summarise them as donor-spread bands.

    A thin wrapper over `simulate_household_total_draws` that collapses the complete
    joint draws to per-household point estimates and central-`level` bands. Use
    `simulate_household_total_draws` directly when the predictive distribution across
    draws is needed (`distribution_across_draws`), since the cross-section of these
    per-household medians is not that distribution.

    Args:
        recipients: Columns `p_id`, `hh_id`, `survey_year`; one row per recipient,
            aligned to every config's per-recipient arrays.
        configs: One `ComponentDrawConfig` per modelled component.
        n_draws: Number of complete joint draws (>= 1).
        rng: NumPy random generator threaded through all draws.
        level: Central level of the reported donor-spread band.
        residual_config: If given, the signed unmodelled-components residual is drawn by
            PMM each draw and added to the total, so its spread enters the band.

    Returns:
        Columns `hh_id`, `survey_year`, `point_estimate`, `lower`, `upper` (float64).

    Raises:
        ValueError: On missing recipient keys, no configs, `n_draws < 1`, or a config
            whose arrays do not align with the recipients.
    """
    draws = simulate_household_total_draws(
        recipients,
        configs,
        n_draws=n_draws,
        rng=rng,
        residual_config=residual_config,
    )
    return household_total_intervals(draws, level=level)


def _draw_all_components(
    configs: Sequence[ComponentDrawConfig],
    rng: np.random.Generator,
    *,
    caliper: float | None = None,
) -> dict[CanonicalComponent, ComponentDraw]:
    """Draw every component for one complete joint draw, coupling secured pairs.

    A secured liability (a config whose component is in `SECURED_BY`) is drawn jointly
    with its backing asset via `_draw_secured_housing`, so the liability is zero
    wherever the asset is not owned. Every other component is drawn independently. When
    a secured liability appears without its backing asset among the configs, it is drawn
    independently -- the coupling needs both legs present.

    Args:
        configs: One `ComponentDrawConfig` per modelled component.
        rng: NumPy random generator threaded through the draws.
        caliper: Optional maximum donor score distance, threaded to every component's
            `draw_component`. `None` (the default) draws the nearest donor regardless of
            distance.

    Returns:
        One `ComponentDraw` per config, keyed by its canonical component.
    """
    config_by_component = {config.component: config for config in configs}
    drawn: dict[CanonicalComponent, ComponentDraw] = {}
    for config in configs:
        backing = SECURED_BY.get(config.component)
        backing_config = (
            config_by_component.get(backing) if backing is not None else None
        )
        if backing is not None and backing_config is not None:
            asset_draw, liability_draw = _draw_secured_housing(
                backing_config, config, rng, caliper=caliper
            )
            drawn[backing] = asset_draw
            drawn[config.component] = liability_draw
        elif config.component not in drawn:
            drawn[config.component] = _draw_config(config, rng, caliper=caliper)
    return drawn


def _draw_config(
    config: ComponentDrawConfig,
    rng: np.random.Generator,
    *,
    caliper: float | None = None,
) -> ComponentDraw:
    """Draw one component independently from its config."""
    return draw_component(
        ownership_prob=config.ownership_prob,
        ownership_share=config.ownership_share,
        recipient_predicted=config.recipient_predicted,
        donor_predicted=config.donor_predicted,
        donor_observed=config.donor_observed,
        scale=config.scale,
        k=config.k,
        rng=rng,
        caliper=caliper,
    )


def _draw_secured_housing(
    property_config: ComponentDrawConfig,
    mortgage_config: ComponentDrawConfig,
    rng: np.random.Generator,
    *,
    caliper: float | None = None,
) -> tuple[ComponentDraw, ComponentDraw]:
    """Draw the property/mortgage pair as one donor bundle: an observed pair always.

    The gross property is drawn first; the mortgage then rides along with the *same*
    matched donor household -- its amount is that donor's own mortgage
    (`property_config.paired_liability_observed`), zero when the donor has no mortgage
    and zero for every recipient drawn as a non-property-owner. Because the (property,
    mortgage) pair is copied from one observed household, the recipient's net home
    equity can never fall outside the donor support: independently matching a
    low-property donor to a high-mortgage donor -- the recombination that manufactures
    unsupported negative
    equity -- cannot happen.

    The mortgage's own incidence and amount models are therefore not consulted for the
    coupled draw; the mortgage config is still used for its ownership share and for the
    marginal support diagnostics.

    Args:
        property_config: The gross owner-occupied-property config (the backing asset).
            Its `paired_liability_observed` carries each property donor's own mortgage,
            aligned to `donor_observed`; required for the coupled draw.
        mortgage_config: The owner-occupied-mortgage config (the secured liability); its
            `ownership_share` resolves the drawn gross mortgage to a person value.
        rng: NumPy random generator; the property leg draws the donor for both legs.
        caliper: Optional maximum donor score distance, threaded to the property draw.

    Returns:
        The property and mortgage `ComponentDraw`s; the mortgage amount is the matched
        property donor's own mortgage, zero for non-owners and for owners whose donor
        carries no mortgage.

    Raises:
        ValueError: If `property_config.paired_liability_observed` is `None`, since the
            coupled draw cannot source a coherent mortgage without it.
    """
    paired = property_config.paired_liability_observed
    if paired is None:
        msg = (
            "property_config.paired_liability_observed is required to draw the secured "
            "mortgage from the matched property donor."
        )
        raise ValueError(msg)
    paired = np.asarray(paired, dtype="float64")
    property_draw = _draw_config(property_config, rng, caliper=caliper)
    owns_property = property_draw.owns

    gross_amount = np.zeros_like(property_draw.gross_amount)
    gross_amount[owns_property] = paired[property_draw.donor_indices[owns_property]]
    owns_mortgage = owns_property & (gross_amount > 0.0)

    person_value = np.zeros_like(gross_amount)
    if owns_mortgage.any():
        person_value[owns_mortgage] = resolve_person_amount(
            pd.Series(gross_amount[owns_mortgage]),
            pd.Series(mortgage_config.ownership_share[owns_mortgage]),
        ).to_numpy()

    mortgage_draw = ComponentDraw(
        person_value=person_value,
        owns=owns_mortgage,
        gross_amount=gross_amount,
        # The mortgage shares the property leg's donor, so it shares its match distance
        # (NaN where no mortgage is drawn, matching the non-owner convention).
        distances=np.where(owns_mortgage, property_draw.distances, np.nan),
        donor_indices=np.where(owns_mortgage, property_draw.donor_indices, -1),
    )
    return property_draw, mortgage_draw


def nearest_donor_distances(
    configs: Sequence[ComponentDrawConfig],
) -> dict[str, np.ndarray]:
    """Return each recipient's score distance to its nearest donor, per component.

    The nearest-donor distance is `min_j |recipient_score_i - donor_score_j|`, a
    deterministic support diagnostic that does not depend on the draw RNG or on
    ownership incidence: it measures how far each recipient sits from the closest point
    of the donor pool in predictive-score space. A large distance means the recipient is
    served by an out-of-support donor, the extrapolation that this projection cannot
    avoid while the target wave has no anchoring wealth answers.

    Args:
        configs: One `ComponentDrawConfig` per modelled component.

    Returns:
        A dict keyed by the component value (e.g. `"financial_assets"`), each mapping to
        a float64 array of per-recipient nearest-donor distances.
    """
    return {
        config.component.value: np.min(
            np.abs(
                config.recipient_predicted[:, None] - config.donor_predicted[None, :]
            ),
            axis=1,
        ).astype("float64")
        for config in configs
    }


def collect_donor_wave_composition(
    recipients: pd.DataFrame,
    configs: Sequence[ComponentDrawConfig],
    *,
    n_draws: int,
    rng: np.random.Generator,
    caliper: float | None = None,
) -> dict[str, dict[int, float]]:
    """Return each component's drawn-donor share by historical wave.

    Across all draws and recipients, every drawn donor is attributed to its survey year
    (`ComponentDrawConfig.donor_year`); the shares within a component sum to one. A
    component whose config carries no `donor_year` is omitted, since its draws cannot be
    attributed to a wave. This is a transparency diagnostic: it shows which historical
    waves the target-wave draws lean on, not a fix for the absence of target-wave
    answers.

    Args:
        recipients: Columns `p_id`, `hh_id`, `survey_year`; one row per recipient.
        configs: One `ComponentDrawConfig` per modelled component.
        n_draws: Number of complete joint draws (>= 1).
        rng: NumPy random generator threaded through all draws.
        caliper: Optional maximum donor score distance per draw, matching
            `simulate_household_total_draws`.

    Returns:
        A dict keyed by the component value, each mapping survey year to the share of
        that component's drawn donors sourced from that wave.

    Raises:
        ValueError: On missing recipient keys, no configs, `n_draws < 1`, a config whose
            arrays do not align with the recipients, or a recipient with no donor within
            `caliper`.
    """
    _fail_if_simulation_inputs_invalid(recipients, configs, n_draws)
    wave_counts: dict[str, dict[int, int]] = {
        config.component.value: {}
        for config in configs
        if config.donor_year is not None
    }
    for _ in range(n_draws):
        for config in configs:
            drawn = draw_component(
                ownership_prob=config.ownership_prob,
                ownership_share=config.ownership_share,
                recipient_predicted=config.recipient_predicted,
                donor_predicted=config.donor_predicted,
                donor_observed=config.donor_observed,
                scale=config.scale,
                k=config.k,
                rng=rng,
                caliper=caliper,
            )
            if config.donor_year is None:
                continue
            drawn_donors = drawn.donor_indices[drawn.owns]
            drawn_waves = np.asarray(config.donor_year)[drawn_donors]
            counts = wave_counts[config.component.value]
            years, year_counts = np.unique(drawn_waves, return_counts=True)
            for year, count in zip(years, year_counts, strict=True):
                counts[int(year)] = counts.get(int(year), 0) + int(count)
    return {
        component: _shares_from_counts(counts)
        for component, counts in wave_counts.items()
    }


def _shares_from_counts(counts: dict[int, int]) -> dict[int, float]:
    """Normalise integer wave counts to shares summing to one (empty stays empty)."""
    total = sum(counts.values())
    if total == 0:
        return {}
    return {year: count / total for year, count in counts.items()}


def _fail_if_simulation_inputs_invalid(
    recipients: pd.DataFrame,
    configs: Sequence[ComponentDrawConfig],
    n_draws: int,
) -> None:
    missing = [key for key in _RECIPIENT_KEYS if key not in recipients.columns]
    if missing:
        msg = f"recipients is missing required columns: {missing}"
        raise ValueError(msg)
    if not configs:
        msg = "configs must contain at least one component."
        raise ValueError(msg)
    if n_draws < 1:
        msg = f"n_draws must be >= 1, got {n_draws}."
        raise ValueError(msg)
    n_recipients = len(recipients)
    for config in configs:
        if config.ownership_prob.shape[0] != n_recipients:
            msg = (
                f"config for {config.component.value} has "
                f"{config.ownership_prob.shape[0]} recipient entries, but there are "
                f"{n_recipients} recipients."
            )
            raise ValueError(msg)
