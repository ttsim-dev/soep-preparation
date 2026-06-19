"""Predictive-mean-matching donor draws.

Each recipient borrows an observed amount from one of its `k` nearest *eligible*
donors in predictive-score space, so imputed values stay on the real observed
support and the back-transformation of `asinh`-scale predictions never has to be
computed analytically. Eligibility — recipient-specific exclusions and the caliper —
is enforced *before* the nearest `k` are chosen, so an out-of-caliper or excluded
donor can never be drawn.
"""

from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class PmmResult:
    """Outcome of a PMM draw, with donor diagnostics."""

    values: np.ndarray
    """Drawn donor values as float64, shape `(n_recipients,)`."""
    donor_indices: np.ndarray
    """Index into the donor arrays of the drawn donor, shape `(n_recipients,)`."""
    distances: np.ndarray
    """Score distance to the drawn donor as float64, shape `(n_recipients,)`."""


def _fail_if_pmm_inputs_invalid(  # noqa: PLR0913
    recipient_scores: np.ndarray,
    donor_scores: np.ndarray,
    donor_values: np.ndarray,
    k: int,
    caliper: float | None,
    exclude: Sequence[Sequence[int]] | None,
) -> None:
    for name, arr in (
        ("recipient_scores", recipient_scores),
        ("donor_scores", donor_scores),
        ("donor_values", donor_values),
    ):
        if arr.ndim != 1:
            msg = f"{name} must be 1-D, got {arr.ndim}-D"
            raise ValueError(msg)
        if not np.all(np.isfinite(arr)):
            msg = f"{name} must be finite (no NaN/inf)"
            raise ValueError(msg)
    if donor_scores.shape[0] != donor_values.shape[0]:
        msg = (
            "donor_scores and donor_values must have equal length, got "
            f"{donor_scores.shape[0]} and {donor_values.shape[0]}"
        )
        raise ValueError(msg)
    if donor_scores.shape[0] == 0:
        msg = "donors must be non-empty"
        raise ValueError(msg)
    if k < 1:
        msg = f"k must be >= 1, got {k}"
        raise ValueError(msg)
    if caliper is not None and (not np.isfinite(caliper) or caliper < 0):
        msg = f"caliper must be finite and non-negative, got {caliper}"
        raise ValueError(msg)
    if exclude is not None and len(exclude) != recipient_scores.shape[0]:
        msg = (
            "exclude must have one entry per recipient, got "
            f"{len(exclude)} for {recipient_scores.shape[0]} recipients"
        )
        raise ValueError(msg)


def pmm_draw(  # noqa: PLR0913
    recipient_scores: np.ndarray,
    donor_scores: np.ndarray,
    donor_values: np.ndarray,
    k: int,
    rng: np.random.Generator,
    caliper: float | None = None,
    exclude: Sequence[Sequence[int]] | None = None,
) -> PmmResult:
    """Draw one near, eligible donor's observed value per recipient.

    For each recipient the donor set is filtered to the eligible candidates — those
    not in that recipient's `exclude` list and (if a `caliper` is given) within it —
    *before* the nearest `k` are selected and one is drawn with `rng`. This
    guarantees an out-of-caliper or excluded donor is never returned.

    Args:
        recipient_scores: Finite predictive scores, shape `(n_recipients,)`.
        donor_scores: Finite predictive scores, shape `(n_donors,)`.
        donor_values: Finite observed values, shape `(n_donors,)`.
        k: Number of nearest eligible donors to sample from (>= 1).
        rng: NumPy random generator.
        caliper: Maximum allowed score distance to a donor, if set (>= 0).
        exclude: Per-recipient sequences of donor indices to exclude, if any.

    Returns:
        A `PmmResult` with drawn values (float64), donor indices, and distances.

    Raises:
        ValueError: On invalid inputs, or if a recipient has no eligible donor.
    """
    _fail_if_pmm_inputs_invalid(
        recipient_scores, donor_scores, donor_values, k, caliper, exclude
    )
    all_indices = np.arange(donor_scores.shape[0])
    n_recipients = recipient_scores.shape[0]
    values = np.empty(n_recipients, dtype=np.float64)
    donor_indices = np.empty(n_recipients, dtype=np.intp)
    distances_out = np.empty(n_recipients, dtype=np.float64)
    for i, score in enumerate(recipient_scores):
        eligible = all_indices
        if exclude is not None:
            excluded = np.asarray(tuple(exclude[i]), dtype=np.intp)
            eligible = eligible[~np.isin(eligible, excluded)]
        distances = np.abs(donor_scores[eligible] - score)
        if caliper is not None:
            within = distances <= caliper
            eligible = eligible[within]
            distances = distances[within]
        if eligible.shape[0] == 0:
            msg = f"No eligible donor within caliper {caliper} for recipient {i}."
            raise ValueError(msg)
        nearest = np.argsort(distances)[:k]
        chosen = rng.choice(nearest)
        donor_indices[i] = eligible[chosen]
        values[i] = float(donor_values[eligible[chosen]])
        distances_out[i] = float(distances[chosen])
    return PmmResult(
        values=values, donor_indices=donor_indices, distances=distances_out
    )
