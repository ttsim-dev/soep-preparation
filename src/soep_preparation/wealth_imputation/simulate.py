"""Joint-draw simulation of household wealth totals with donor-spread bands.

Each draw runs every component's sequential `draw_component`, sums the signed
person-component values to a household net total, and the spread of those totals
across draws becomes the band. Summing within each draw before taking the spread is
the correct way to aggregate -- component band endpoints are never summed.

The total is an **independent conditional recombination (sensitivity)**, not a coherent
joint draw: each component and the accounting residual are drawn independently given the
covariates, so the only cross-component dependence carried into the total is through
shared covariates -- the empirical co-movement of the components within a real household
is not reproduced, and the household accounting law that ties the components to the
official total is not enforced draw by draw. The resulting bands are therefore
**conditional donor-randomisation spreads**, not calibrated predictive intervals: they
hold the fitted models and the chosen implicate fixed (the residual *model* is fixed but
its donor value is redrawn each draw). One shared RNG threads all draws so a fixed seed
reproduces the whole simulation.
"""

# REVIEW (F4): components and the accounting residual are drawn independently, so the
# joint accounting law and the empirical component co-movement are not preserved. A
# coherent fix draws a whole donor bundle (all components of one observed household)
# together rather than recombining marginals; left for human decision.

from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
import pandas as pd

from soep_preparation.wealth_imputation.aggregate import household_net_total
from soep_preparation.wealth_imputation.components import CanonicalComponent
from soep_preparation.wealth_imputation.donors import pmm_draw
from soep_preparation.wealth_imputation.intervals import household_total_intervals
from soep_preparation.wealth_imputation.value_generator import draw_component

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


def simulate_household_total_draws(
    recipients: pd.DataFrame,
    configs: Sequence[ComponentDrawConfig],
    *,
    n_draws: int,
    rng: np.random.Generator,
    residual_config: ResidualDrawConfig | None = None,
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

    Returns:
        Columns `hh_id`, `survey_year`, `household_total_draw` (float64), the
        component-only total; `n_draws` rows per household, one per complete draw. When
        `residual_config` is given, an extra `residual_inclusive_total_draw` column adds
        the per-draw signed residual to the component-only total (the same draw, so the
        two totals are household-consistent). Draw membership is the within-household
        row order, as `distribution_across_draws` expects.

    Raises:
        ValueError: On missing recipient keys, no configs, `n_draws < 1`, or a config
            whose arrays do not align with the recipients.
    """
    _fail_if_simulation_inputs_invalid(recipients, configs, n_draws)
    keys = recipients[list(_RECIPIENT_KEYS)]
    draw_frames = []
    for _ in range(n_draws):
        component_rows = []
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
            )
            component_rows.append(
                keys[["hh_id", "survey_year"]].assign(
                    component=config.component.value,
                    household_component_value=drawn.person_value,
                )
            )
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
