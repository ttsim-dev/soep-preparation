"""Convert jointly-held asset totals to per-person amounts via ownership shares.

Applied exactly once, when building the person-level panel from raw answers. The
resulting `person_component_value` is final and share-resolved, so household
aggregation downstream is a plain sum (never a second share multiply). Both operands
are promoted to `float64` *before* the multiplication, so a lower-precision input dtype
cannot silently lose precision.
"""

import numpy as np
import pandas as pd


def _fail_if_share_values_invalid(amount: pd.Series, share: pd.Series) -> None:
    observed_share = share.to_numpy()
    observed_share = observed_share[~np.isnan(observed_share)]
    if observed_share.size and not np.isfinite(observed_share).all():
        msg = "ownership_share must be finite where observed."
        raise ValueError(msg)
    if observed_share.size and np.any((observed_share < 0) | (observed_share > 1)):
        msg = "observed ownership_share must lie in [0, 1]."
        raise ValueError(msg)
    observed_amount = amount.to_numpy()
    observed_amount = observed_amount[~np.isnan(observed_amount)]
    if observed_amount.size and not np.isfinite(observed_amount).all():
        msg = "joint_amount must be finite where observed."
        raise ValueError(msg)


def resolve_person_amount(
    joint_amount: pd.Series, ownership_share: pd.Series
) -> pd.Series:
    """Return the person's share of a jointly-held asset value.

    Args:
        joint_amount: Total value of the asset, same index as `ownership_share`;
            finite where observed.
        ownership_share: The person's ownership share in `[0, 1]` where observed;
            NA if unknown.

    Returns:
        `joint_amount * ownership_share` computed in float64, with NA propagated where
        either input is NA.

    Raises:
        ValueError: On a mismatched index, a non-numeric or non-finite observed value,
            or an observed share outside `[0, 1]`.
    """
    if not joint_amount.index.equals(ownership_share.index):
        msg = "joint_amount and ownership_share must share an identical index."
        raise ValueError(msg)
    amount = pd.to_numeric(joint_amount, errors="raise").astype("float64")
    share = pd.to_numeric(ownership_share, errors="raise").astype("float64")
    _fail_if_share_values_invalid(amount, share)
    return amount * share
