"""Joint-draw simulation of household wealth totals with donor-spread bands.

Each draw runs every component's sequential `draw_component`, sums the signed
person-component values to a household net total, and the spread of those totals
across draws becomes the band. Summing within each draw before taking the spread is
the correct way to aggregate -- component band endpoints are never summed.

The resulting bands are **conditional donor-randomisation spreads**, not calibrated
predictive intervals: given the fitted models and covariates, the components are drawn
independently (the only cross-component dependence is through shared covariates, not a
modelled component covariance), and the draws hold the fitted models and the chosen
implicate fixed -- the residual *model* is fixed but its donor value is redrawn each
draw. One shared RNG threads all draws so a fixed seed reproduces the whole simulation.
"""

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
            totals["household_total_draw"] = (
                totals["household_total_draw"] + totals["residual_draw"]
            )
            totals = totals.drop(columns="residual_draw")
        draw_frames.append(totals)
    all_draws = pd.concat(draw_frames, ignore_index=True)
    return household_total_intervals(all_draws, level=level)


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
