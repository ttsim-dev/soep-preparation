"""Draw component amounts by predictive mean matching on the asinh-scaled axis.

The amount model's `predict_score` already lives on the `asinh(y / s_c)` axis, so
recipients and donors are matched there directly -- no further transform, and in
particular no `sinh`-then-`asinh` round trip that overflows at the support boundary.
Each recipient takes the **observed** euro value of its drawn donor, so every imputed
amount stays on the real support without back-transforming a prediction. The component
scale is carried only for caliper interpretation.
"""

from collections.abc import Sequence

import numpy as np

from soep_preparation.wealth_imputation.donors import PmmResult, pmm_draw


def draw_amounts(  # noqa: PLR0913
    recipient_predicted: np.ndarray,
    donor_predicted: np.ndarray,
    donor_observed: np.ndarray,
    scale: float,  # noqa: ARG001 -- kept for a uniform component-draw signature
    k: int,
    rng: np.random.Generator,
    caliper: float | None = None,
    exclude: Sequence[Sequence[int]] | None = None,
) -> PmmResult:
    """Draw each recipient's amount from a near donor's observed value.

    Args:
        recipient_predicted: Recipient matching scores on the asinh axis
            (`AmountModel.predict_score`), shape `(n_recipients,)`.
        donor_predicted: Donor matching scores on the asinh axis, shape `(n_donors,)`.
        donor_observed: Observed euro amounts for donors, shape `(n_donors,)`.
        scale: Component scale `s_c`; retained for signature uniformity. The scores are
            already on the asinh axis, so no transform is applied here.
        k: Number of nearest eligible donors to sample from (>= 1).
        rng: NumPy random generator.
        caliper: Maximum allowed distance on the asinh-scaled axis, if set (>= 0).
        exclude: Per-recipient sequences of donor indices to exclude, if any.

    Returns:
        A `PmmResult` whose `values` are the drawn observed euro amounts (float64).

    Raises:
        ValueError: On invalid PMM inputs or a recipient with no eligible donor.
    """
    return pmm_draw(
        recipient_scores=np.asarray(recipient_predicted, dtype="float64"),
        donor_scores=np.asarray(donor_predicted, dtype="float64"),
        donor_values=np.asarray(donor_observed, dtype="float64"),
        k=k,
        rng=rng,
        caliper=caliper,
        exclude=exclude,
    )
