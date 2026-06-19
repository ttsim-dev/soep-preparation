"""Aggregate share-resolved person amounts to households and isolate PUNR residuals.

Household wealth is the plain sum of members' share-resolved person amounts plus a
household-level partial-unit-nonresponse (PUNR) residual — the part of the official
household total not covered by represented members.
"""

import pandas as pd


def household_component_sum(person_panel: pd.DataFrame) -> pd.DataFrame:
    """Sum share-resolved person amounts within household, year, and component.

    Args:
        person_panel: Columns `hh_id`, `survey_year`, `component`,
            `person_component_value`.

    Returns:
        Columns `hh_id`, `survey_year`, `component`, `member_sum`.

    """
    return (
        person_panel.groupby(["hh_id", "survey_year", "component"], observed=True)[
            "person_component_value"
        ]
        .sum()
        .reset_index(name="member_sum")
    )


def punr_residual(household_total: pd.Series, member_sum: pd.Series) -> pd.Series:
    """Return the household total minus the represented-member sum.

    Args:
        household_total: Official household component total.
        member_sum: Sum of represented members' share-resolved amounts.

    Returns:
        The residual to be attributed to partial-unit nonresponse.

    """
    return household_total - member_sum
