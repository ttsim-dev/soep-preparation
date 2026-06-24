"""Generate a per-component person value by chaining ownership, amount, and share.

For one component, each recipient's draw runs in sequence: a Bernoulli ownership
decision from its incidence probability, then -- for owners only -- a PMM amount draw
(`amounts.draw_amounts`) and a single ownership-share resolution
(`shares.resolve_person_amount`). Non-owners contribute a structural zero. Ownership is
drawn before amounts so a fixed seed reproduces the whole chain, and passing 0/1
probabilities makes the ownership step a deterministic filter.
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd

from soep_preparation.wealth_imputation.amounts import draw_amounts
from soep_preparation.wealth_imputation.shares import resolve_person_amount


@dataclass(frozen=True)
class ComponentDraw:
    """Outcome of one component's sequential draw over recipients."""

    person_value: np.ndarray
    """Final share-resolved person amount per recipient as float64."""
    owns: np.ndarray
    """Bernoulli ownership decision per recipient as bool."""
    gross_amount: np.ndarray
    """Drawn gross (joint) amount per recipient as float64; zero for non-owners."""


def _fail_if_lengths_differ(
    ownership_prob: np.ndarray,
    ownership_share: np.ndarray,
    recipient_predicted: np.ndarray,
) -> None:
    lengths = {
        "ownership_prob": ownership_prob.shape[0],
        "ownership_share": ownership_share.shape[0],
        "recipient_predicted": recipient_predicted.shape[0],
    }
    if len(set(lengths.values())) > 1:
        msg = f"recipient arrays must share one length, got {lengths}."
        raise ValueError(msg)


def _fail_if_probabilities_invalid(ownership_prob: np.ndarray) -> None:
    if not np.all(np.isfinite(ownership_prob)):
        msg = "ownership_prob must be finite (no NaN/inf)."
        raise ValueError(msg)
    if np.any((ownership_prob < 0.0) | (ownership_prob > 1.0)):
        msg = "ownership_prob must lie in [0, 1]."
        raise ValueError(msg)


def draw_component(  # noqa: PLR0913
    ownership_prob: np.ndarray,
    ownership_share: np.ndarray,
    recipient_predicted: np.ndarray,
    donor_predicted: np.ndarray,
    donor_observed: np.ndarray,
    scale: float,
    k: int,
    rng: np.random.Generator,
    caliper: float | None = None,
) -> ComponentDraw:
    """Draw one component's person value for every recipient.

    Args:
        ownership_prob: Ownership incidence probability per recipient, in `[0, 1]`.
        ownership_share: Ownership share per recipient, in `[0, 1]` for owners.
        recipient_predicted: Recipient matching score on the asinh axis (PMM matching).
        donor_predicted: Donor matching scores on the asinh axis.
        donor_observed: Observed gross euro amounts for donors.
        scale: Positive, finite component scale for the asinh transform.
        k: Number of nearest eligible donors to sample from (>= 1).
        rng: NumPy random generator (ownership drawn first, then amounts).
        caliper: Maximum donor distance on the asinh-scaled axis, if set.

    Returns:
        A `ComponentDraw` with the share-resolved `person_value`, the `owns`
        decision, and the drawn `gross_amount`.

    Raises:
        ValueError: On mismatched recipient lengths, probabilities outside `[0, 1]`,
            an invalid scale, or invalid PMM / share inputs.
    """
    _fail_if_lengths_differ(ownership_prob, ownership_share, recipient_predicted)
    _fail_if_probabilities_invalid(ownership_prob)
    n_recipients = ownership_prob.shape[0]
    owns = rng.random(n_recipients) < ownership_prob
    gross_amount = np.zeros(n_recipients, dtype="float64")
    person_value = np.zeros(n_recipients, dtype="float64")
    if owns.any():
        drawn = draw_amounts(
            recipient_predicted=recipient_predicted[owns],
            donor_predicted=donor_predicted,
            donor_observed=donor_observed,
            scale=scale,
            k=k,
            rng=rng,
            caliper=caliper,
        )
        gross_amount[owns] = drawn.values  # noqa: PD011 -- PmmResult attr, not pandas
        owner_value = resolve_person_amount(
            pd.Series(gross_amount[owns]), pd.Series(ownership_share[owns])
        )
        person_value[owns] = owner_value.to_numpy()
    return ComponentDraw(
        person_value=person_value, owns=owns, gross_amount=gross_amount
    )
