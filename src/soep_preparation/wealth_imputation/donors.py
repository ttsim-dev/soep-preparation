"""Predictive-mean-matching donor draws.

Each recipient borrows an observed amount from one of its `k` nearest donors in
predictive-score space, so imputed values stay on the real observed support and the
back-transformation of `asinh`-scale predictions never has to be computed analytically.
"""

import numpy as np


def pmm_draw(  # noqa: PLR0913
    recipient_scores: np.ndarray,
    donor_scores: np.ndarray,
    donor_values: np.ndarray,
    k: int,
    rng: np.random.Generator,
    caliper: float | None = None,
) -> np.ndarray:
    """Draw one near-donor observed value per recipient.

    Args:
        recipient_scores: Predictive scores for recipients, shape `(n_recipients,)`.
        donor_scores: Predictive scores for donors, shape `(n_donors,)`.
        donor_values: Observed values for donors, shape `(n_donors,)`.
        k: Number of nearest donors to sample from.
        rng: NumPy random generator.
        caliper: If set, the maximum allowed score distance to a donor.

    Returns:
        One drawn value per recipient, shape `(n_recipients,)`.

    Raises:
        ValueError: If a recipient has no donor within `caliper`.
    """
    drawn = np.empty(recipient_scores.shape[0], dtype=donor_values.dtype)
    for i, score in enumerate(recipient_scores):
        distances = np.abs(donor_scores - score)
        order = np.argsort(distances)[:k]
        if caliper is not None and distances[order[0]] > caliper:
            msg = f"No donor within caliper {caliper} for recipient {i}."
            raise ValueError(msg)
        chosen = rng.choice(order)
        drawn[i] = donor_values[chosen]
    return drawn
