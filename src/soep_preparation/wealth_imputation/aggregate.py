"""Aggregate share-resolved person amounts to households and isolate PUNR residuals.

Household wealth is the plain sum of members' share-resolved person amounts plus a
household-level partial-unit-nonresponse (PUNR) residual — the part of the official
household total not covered by represented members. Both helpers fail closed on an
invalid input (missing keys, duplicate cells, non-numeric or non-finite values, or
non-canonical components), and all monetary arithmetic runs in `float64` so input dtype
can neither silently lose precision nor concatenate strings into a plausible total.
"""

import numpy as np
import pandas as pd

from soep_preparation.wealth_imputation.components import CanonicalComponent

_PERSON_PANEL_COLUMNS = (
    "p_id",
    "hh_id",
    "survey_year",
    "component",
    "person_component_value",
)
_KEY_COLUMNS = ["hh_id", "survey_year", "component"]


def _fail_if_columns_missing(
    frame: pd.DataFrame, columns: tuple[str, ...], name: str
) -> None:
    missing = [col for col in columns if col not in frame.columns]
    if missing:
        msg = f"{name} is missing required columns: {missing}"
        raise ValueError(msg)


def _fail_if_components_non_canonical(component_values: pd.Series) -> None:
    valid = {member.value for member in CanonicalComponent}
    bad = set(component_values.dropna().unique()) - valid
    if bad:
        listed = sorted(str(value) for value in bad)
        msg = (
            f"'component' has non-canonical values: {listed}; use "
            "CanonicalComponent.value strings."
        )
        raise ValueError(msg)


def _to_finite_float64(values: pd.Series, name: str) -> pd.Series:
    numeric = pd.to_numeric(values, errors="raise").astype("float64")
    if not np.isfinite(numeric.to_numpy()).all():
        msg = f"{name} must be finite (no NaN/inf)."
        raise ValueError(msg)
    return numeric


def _fail_if_person_panel_invalid(person_panel: pd.DataFrame) -> None:
    _fail_if_columns_missing(person_panel, _PERSON_PANEL_COLUMNS, "person_panel")
    for col in ("p_id", "hh_id", "survey_year", "component"):
        if person_panel[col].isna().any():
            msg = (
                f"person_panel has missing values in key column '{col}'; rows with a "
                "missing key would be silently dropped by groupby."
            )
            raise ValueError(msg)
    _fail_if_components_non_canonical(person_panel["component"])
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

    The values are coerced to `float64` *before* grouping, so integer overflow,
    `float32` precision loss, and string concatenation cannot corrupt the total.

    Args:
        person_panel: Columns `p_id`, `hh_id`, `survey_year`, `component`,
            `person_component_value`. Every key column must be non-missing, each
            `(p_id, survey_year, component)` unique, `component` a
            `CanonicalComponent.value` string, and `person_component_value` a finite
            number.

    Returns:
        Columns `hh_id`, `survey_year`, `component`, `member_sum` (float64).

    Raises:
        ValueError: On missing columns/keys, non-canonical components, duplicate
            person-component rows, or non-numeric / non-finite values.
    """
    _fail_if_person_panel_invalid(person_panel)
    panel = person_panel.assign(
        person_component_value=_to_finite_float64(
            person_panel["person_component_value"], "person_component_value"
        )
    )
    grouped = (
        panel.groupby(_KEY_COLUMNS, observed=True)["person_component_value"]
        .sum()
        .reset_index(name="member_sum")
    )
    grouped["member_sum"] = grouped["member_sum"].astype("float64")
    return grouped


def punr_residual(
    household_totals: pd.DataFrame, member_sums: pd.DataFrame
) -> pd.DataFrame:
    """Return household total minus represented-member sum, joined on household keys.

    The two tables are merged one-to-one on `(hh_id, survey_year, component)`; the keys
    must be unique on each side and identical across the two, so alignment is by
    household identity, not by row position. The residual is positive when the official
    total exceeds the represented-member sum (partial-unit nonresponse), in the same
    sign convention as the inputs (debt carried as positive magnitudes).

    Args:
        household_totals: Columns `hh_id`, `survey_year`, `component`,
            `household_total`.
        member_sums: Columns `hh_id`, `survey_year`, `component`, `member_sum`.

    Returns:
        Columns `hh_id`, `survey_year`, `component`, `punr_residual` (float64).

    Raises:
        ValueError: On missing columns, non-unique keys, key sets that differ between
            the two tables, or non-numeric / non-finite values.
    """
    _fail_if_columns_missing(
        household_totals, (*_KEY_COLUMNS, "household_total"), "household_totals"
    )
    _fail_if_columns_missing(member_sums, (*_KEY_COLUMNS, "member_sum"), "member_sums")
    for name, frame in (
        ("household_totals", household_totals),
        ("member_sums", member_sums),
    ):
        if frame.duplicated(subset=_KEY_COLUMNS).any():
            msg = f"{name} has duplicate (hh_id, survey_year, component) keys."
            raise ValueError(msg)
    merged = household_totals.merge(
        member_sums, on=_KEY_COLUMNS, how="outer", validate="one_to_one", indicator=True
    )
    if (merged["_merge"] != "both").any():
        msg = (
            "household_totals and member_sums must cover identical "
            "(hh_id, survey_year, component) keys."
        )
        raise ValueError(msg)
    total = _to_finite_float64(merged["household_total"], "household_total")
    member = _to_finite_float64(merged["member_sum"], "member_sum")
    out = merged[_KEY_COLUMNS].copy()
    out["punr_residual"] = (total - member).to_numpy()
    return out
