"""Draw component amounts by predictive mean matching on the asinh-scaled scale.

The amount model predicts on the `asinh(y / s_c)` scale, so recipients and donors are
matched there: both predicted amounts are transformed with the component scale `s_c`,
matched by `pmm_draw`, and each recipient takes the **observed** euro value of its drawn
donor. Matching on the scaled axis makes the donor caliper comparable across components
regardless of their euro magnitude, and drawing an observed value keeps every imputed
amount on the real support without back-transforming a prediction.
"""

from collections.abc import Sequence

import numpy as np
import pandas as pd

from soep_preparation.wealth_imputation.donors import PmmResult, pmm_draw
from soep_preparation.wealth_imputation.transforms import asinh_scaled


def draw_amounts(  # noqa: PLR0913
    recipient_predicted: np.ndarray,
    donor_predicted: np.ndarray,
    donor_observed: np.ndarray,
    scale: float,
    k: int,
    rng: np.random.Generator,
    caliper: float | None = None,
    exclude: Sequence[Sequence[int]] | None = None,
) -> PmmResult:
    """Draw each recipient's amount from a near donor's observed value.

    Args:
        recipient_predicted: Predicted euro amounts for recipients, shape
            `(n_recipients,)`.
        donor_predicted: Predicted euro amounts for donors, shape `(n_donors,)`.
        donor_observed: Observed euro amounts for donors, shape `(n_donors,)`.
        scale: Positive, finite component scale `s_c` for the asinh transform.
        k: Number of nearest eligible donors to sample from (>= 1).
        rng: NumPy random generator.
        caliper: Maximum allowed distance on the asinh-scaled axis, if set (>= 0).
        exclude: Per-recipient sequences of donor indices to exclude, if any.

    Returns:
        A `PmmResult` whose `values` are the drawn observed euro amounts (float64).

    Raises:
        ValueError: On an invalid scale, invalid PMM inputs, or a recipient with no
            eligible donor.
    """
    recipient_scores = asinh_scaled(pd.Series(recipient_predicted), scale).to_numpy()
    donor_scores = asinh_scaled(pd.Series(donor_predicted), scale).to_numpy()
    return pmm_draw(
        recipient_scores=recipient_scores,
        donor_scores=donor_scores,
        donor_values=np.asarray(donor_observed, dtype="float64"),
        k=k,
        rng=rng,
        caliper=caliper,
        exclude=exclude,
    )
