"""Aggregate share-resolved person amounts to households and isolate PUNR residuals.

Household wealth is the plain sum of members' share-resolved person amounts plus a
household-level partial-unit-nonresponse (PUNR) residual — the part of the official
household total not covered by represented members. Aggregation fails closed on an
invalid person panel (duplicate person-component rows or missing represented values),
so silent double-counting or a fake PUNR residual cannot arise.
"""

import pandas as pd

_PERSON_PANEL_COLUMNS = (
    "p_id",
    "hh_id",
    "survey_year",
    "component",
    "person_component_value",
)


def _fail_if_person_panel_invalid(person_panel: pd.DataFrame) -> None:
    missing = [col for col in _PERSON_PANEL_COLUMNS if col not in person_panel.columns]
    if missing:
        msg = f"person_panel is missing required columns: {missing}"
        raise ValueError(msg)
    if person_panel.duplicated(subset=["p_id", "survey_year", "component"]).any():
        msg = (
            "person_panel has duplicate (p_id, survey_year, component) rows; each "
            "person-component cell must be unique before aggregation."
        )
        raise ValueError(msg)
    if person_panel["person_component_value"].isna().any():
        msg = (
            "person_panel has missing person_component_value entries; resolve item "
            "nonresponse before aggregation so it is not mistaken for a PUNR residual."
        )
        raise ValueError(msg)


def household_component_sum(person_panel: pd.DataFrame) -> pd.DataFrame:
    """Sum share-resolved person amounts within household, year, and component.

    Args:
        person_panel: Columns `p_id`, `hh_id`, `survey_year`, `component`,
            `person_component_value`. Each `(p_id, survey_year, component)` must be
            unique and `person_component_value` must be non-missing.

    Returns:
        Columns `hh_id`, `survey_year`, `component`, `member_sum` (float64).

    Raises:
        ValueError: If required columns are missing, person-component rows are
            duplicated, or person values are missing.
    """
    _fail_if_person_panel_invalid(person_panel)
    grouped = (
        person_panel.groupby(["hh_id", "survey_year", "component"], observed=True)[
            "person_component_value"
        ]
        .sum()
        .reset_index(name="member_sum")
    )
    grouped["member_sum"] = grouped["member_sum"].astype("float64")
    return grouped


def punr_residual(household_total: pd.Series, member_sum: pd.Series) -> pd.Series:
    """Return the household total minus the represented-member sum.

    Both series must share an identical index so the subtraction is positional, not
    re-aligned by pandas. The residual is positive when the official household total
    exceeds the represented-member sum (partial-unit nonresponse), in the same sign
    convention as the inputs (debt carried as positive magnitudes).

    Args:
        household_total: Official household component total, keyed.
        member_sum: Sum of represented members' share-resolved amounts, same index.

    Returns:
        The residual attributed to partial-unit nonresponse (float64).

    Raises:
        ValueError: If the two series do not share an identical index.
    """
    if not household_total.index.equals(member_sum.index):
        msg = "household_total and member_sum must share an identical index."
        raise ValueError(msg)
    return (household_total - member_sum).astype("float64")
