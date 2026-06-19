"""Convert jointly-held asset totals to per-person amounts via ownership shares.

Applied exactly once, when building the person-level panel from raw answers. The
resulting `person_component_value` is final and share-resolved, so household
aggregation downstream is a plain sum (never a second share multiply).
"""

import pandas as pd


def resolve_person_amount(
    joint_amount: pd.Series, ownership_share: pd.Series
) -> pd.Series:
    """Return the person's share of a jointly-held asset value.

    Args:
        joint_amount: Total value of the asset reported for the household/asset.
        ownership_share: The person's ownership share in `[0, 1]`; NA if unknown.

    Returns:
        `joint_amount * ownership_share`, with NA propagated where the share is NA.
    """
    return joint_amount * ownership_share
