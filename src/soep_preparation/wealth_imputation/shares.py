"""Convert jointly-held asset totals to per-person amounts via ownership shares.

Applied exactly once, when building the person-level panel from raw answers. The
resulting `person_component_value` is final and share-resolved, so household
aggregation downstream is a plain sum (never a second share multiply).
"""

import numpy as np
import pandas as pd


def _fail_if_share_inputs_invalid(
    joint_amount: pd.Series, ownership_share: pd.Series
) -> None:
    if not joint_amount.index.equals(ownership_share.index):
        msg = "joint_amount and ownership_share must share an identical index."
        raise ValueError(msg)
    observed_share = ownership_share.dropna().astype("float64").to_numpy()
    if not np.all(np.isfinite(observed_share)):
        msg = "ownership_share must be finite where observed."
        raise ValueError(msg)
    if np.any((observed_share < 0) | (observed_share > 1)):
        msg = "observed ownership_share must lie in [0, 1]."
        raise ValueError(msg)
    observed_amount = joint_amount.dropna().astype("float64").to_numpy()
    if not np.all(np.isfinite(observed_amount)):
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
        `joint_amount * ownership_share` as float64, with NA propagated where either
        input is NA.

    Raises:
        ValueError: On a mismatched index, a non-finite observed value, or an
            observed share outside `[0, 1]`.
    """
    _fail_if_share_inputs_invalid(joint_amount, ownership_share)
    return (joint_amount * ownership_share).astype("float64")
