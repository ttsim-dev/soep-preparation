"""Predictor registry for the wealth models, mapped to cleaned SOEP-Core columns.

Each `FeatureSpec` pins one model predictor to its cleaned output column, the module
that produces it, the level it is measured at (person or household), and whether it is
continuous or categorical. Names are confirmed against both the Milestone-0 covariate
inventory (raw availability) and the clean-module sources (cleaned output), so the
model stage selects predictors by confirmed name rather than guessing. Household-level
predictors are broadcast to persons via `hh_id` during feature assembly.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

import pandas as pd

_PERSON_KEYS = ("p_id", "hh_id", "survey_year")
_HOUSEHOLD_KEYS = ("hh_id", "survey_year")


@dataclass(frozen=True)
class FeatureSpec:
    """One model predictor mapped to its cleaned source column."""

    column: str
    """Cleaned output column name in `source_module`."""
    source_module: str
    """`MODULES` catalog key of the module that produces the column."""
    level: Literal["person", "household"]
    """Whether the predictor is measured per person or per household."""
    kind: Literal["continuous", "categorical"]
    """Whether the predictor enters the model as a number or a category."""


FEATURE_SPECS: tuple[FeatureSpec, ...] = (
    FeatureSpec("age", "pequiv", "person", "continuous"),
    FeatureSpec("gender", "pequiv", "person", "categorical"),
    FeatureSpec("number_of_persons_hh", "pequiv", "person", "continuous"),
    FeatureSpec("number_of_children_living_in_hh", "pequiv", "person", "continuous"),
    FeatureSpec("federal_state_of_residence", "pequiv", "person", "categorical"),
    FeatureSpec("einkommen_nach_steuern_y_hh", "pequiv", "person", "continuous"),
    FeatureSpec("education_isced", "pgen", "person", "categorical"),
    FeatureSpec("employment_status", "pgen", "person", "categorical"),
    FeatureSpec("marital_status", "pgen", "person", "categorical"),
    FeatureSpec("occupation_status", "pgen", "person", "categorical"),
    FeatureSpec("self_employed", "pgen", "person", "categorical"),
    FeatureSpec("retired", "pgen", "person", "categorical"),
    FeatureSpec("gross_labor_income_previous_month_m", "pgen", "person", "continuous"),
    FeatureSpec("tenure", "pgen", "person", "continuous"),
    FeatureSpec("migration_background", "ppathl", "person", "categorical"),
    FeatureSpec("rented_or_owned", "hgen", "household", "categorical"),
    FeatureSpec("living_space_hh", "hgen", "household", "continuous"),
    FeatureSpec("building_year_hh_min", "hgen", "household", "continuous"),
)


def assemble_feature_matrix(modules: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    """Join the cleaned modules into one person-level predictor matrix.

    Person-level modules are merged on `(p_id, hh_id, survey_year)`; household-level
    modules are then left-joined on `(hh_id, survey_year)`, broadcasting each household
    predictor to its members. Only the registered feature columns (plus keys) are kept.

    Args:
        modules: `MODULES` name -> cleaned module frame. Must contain every source
            module in `FEATURE_SPECS` with its required columns.

    Returns:
        One row per person-year with `p_id`, `hh_id`, `survey_year`, and every
        registered feature column.

    Raises:
        ValueError: If a source module or a registered column is missing.
    """
    fail_if_features_missing(modules)
    columns_by_module = required_columns_by_module()
    person_modules = sorted(
        {spec.source_module for spec in FEATURE_SPECS if spec.level == "person"}
    )
    household_modules = sorted(
        {spec.source_module for spec in FEATURE_SPECS if spec.level == "household"}
    )
    person_frames = [
        modules[module][list(columns_by_module[module])] for module in person_modules
    ]
    matrix = person_frames[0]
    for subset in person_frames[1:]:
        matrix = matrix.merge(subset, on=list(_PERSON_KEYS), how="outer")
    for module in household_modules:
        subset = modules[module][list(columns_by_module[module])]
        matrix = matrix.merge(subset, on=list(_HOUSEHOLD_KEYS), how="left")
    return matrix


def required_columns_by_module() -> dict[str, tuple[str, ...]]:
    """Return the columns each source module must provide, including its keys.

    Returns:
        Module name -> the key columns for its level plus every registered feature
        column drawn from that module.
    """
    by_module: dict[str, list[str]] = {}
    for spec in FEATURE_SPECS:
        keys = _PERSON_KEYS if spec.level == "person" else _HOUSEHOLD_KEYS
        columns = by_module.setdefault(spec.source_module, list(keys))
        if spec.column not in columns:
            columns.append(spec.column)
    return {module: tuple(columns) for module, columns in by_module.items()}


def fail_if_features_missing(modules: Mapping[str, pd.DataFrame]) -> None:
    """Raise if any registered feature column is absent from its source module.

    Args:
        modules: `MODULES` name -> cleaned module frame.

    Raises:
        ValueError: If a source module is absent, or a registered column is missing
            from the module that should produce it.
    """
    for module, columns in required_columns_by_module().items():
        if module not in modules:
            msg = f"Required source module '{module}' is absent from modules."
            raise ValueError(msg)
        missing = [
            column for column in columns if column not in modules[module].columns
        ]
        if missing:
            msg = f"Module '{module}' is missing required columns: {missing}"
            raise ValueError(msg)
