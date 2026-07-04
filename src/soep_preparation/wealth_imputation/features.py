"""Predictor registry for the wealth models, mapped to cleaned SOEP-Core columns.

Each `FeatureSpec` pins one model predictor to its cleaned output column, the module
that produces it, the level it is measured at (person or household), and whether it is
continuous or categorical. Names are confirmed against both the Milestone-0 covariate
inventory (raw availability) and the clean-module sources (cleaned output), so the
model stage selects predictors by confirmed name rather than guessing. Household-level
predictors are broadcast to persons via `hh_id` during feature assembly.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

import numpy as np
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
    FeatureSpec("income_after_tax_y_hh", "pequiv", "person", "continuous"),
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


def select_household_heads(frame: pd.DataFrame) -> pd.DataFrame:
    """Keep one representative row -- the oldest member -- per household and year.

    Representing each household by its oldest member lets the simulation aggregate one
    row per household without summing jointly-held amounts across members (which would
    double-count, since SOEP-Core omits the person-level ownership shares).

    A member whose age is missing carries no evidence of being oldest, so it never
    outranks a member with a known age: missing ages sort to the front and the greatest
    known age wins. Equal ages resolve to the lowest `p_id`, so the choice is
    deterministic and independent of input order.

    Args:
        frame: Rows with `p_id`, `hh_id`, `survey_year`, and `age`.

    Returns:
        One row per `(hh_id, survey_year)`, the member with the greatest known `age`
        (lowest `p_id` on a tie).
    """
    ordered = frame.sort_values(
        ["hh_id", "survey_year", "age", "p_id"],
        ascending=[True, True, True, False],
        na_position="first",
    )
    return ordered.drop_duplicates(
        subset=["hh_id", "survey_year"], keep="last"
    ).reset_index(drop=True)


def encode_design_matrix(
    frame: pd.DataFrame,
    columns: Sequence[str],
    *,
    medians: Mapping[str, float] | None = None,
) -> np.ndarray:
    """Return a float64 design matrix, filling missing values with column medians.

    When `medians` is supplied (the training medians carried by a fitted encoder), each
    column's missing values are filled with the *training* median, so a recipient frame
    and a training subset assign the same numeric meaning to a missing covariate. When
    it is not supplied, the per-frame median is used as a fallback for standalone use.

    Args:
        frame: Source frame containing every name in `columns`.
        columns: Predictor columns to encode, in order.
        medians: Optional training median per column.

    Returns:
        A `(len(frame), len(columns))` float64 array; an all-missing column becomes
        zeros.
    """
    encoded = []
    for column in columns:
        values = pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype="float64")
        if medians is not None and column in medians:
            median = medians[column]
        elif np.any(~np.isnan(values)):
            median = float(np.nanmedian(values))
        else:
            median = 0.0
        encoded.append(np.where(np.isnan(values), median, values))
    if not encoded:
        return np.empty((len(frame), 0), dtype="float64")
    return np.column_stack(encoded).astype("float64")


@dataclass(frozen=True)
class CategoricalEncoder:
    """Fixed training statistics for stable one-hot width and consistent NA filling."""

    categories: Mapping[str, tuple[str, ...]]
    """Column name -> the sorted distinct training categories."""
    continuous_medians: Mapping[str, float] = MappingProxyType({})
    """Continuous column name -> its training median, used to fill missing values."""


def fit_categorical_encoder(
    frame: pd.DataFrame,
    columns: Sequence[str],
    *,
    continuous_columns: Sequence[str] = (),
) -> CategoricalEncoder:
    """Record fixed training categories and continuous medians for later encoding.

    Args:
        frame: Training frame containing every name in `columns`.
        columns: Categorical predictor columns.
        continuous_columns: Continuous predictor columns whose training median is
            stored so recipients are filled with the training (not their own) median.

    Returns:
        A `CategoricalEncoder` with one fixed category tuple per categorical column and
        one training median per continuous column.
    """
    categories = {
        column: tuple(sorted(frame[column].dropna().astype(str).unique()))
        for column in columns
    }
    medians = {}
    for column in continuous_columns:
        values = pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype="float64")
        medians[column] = (
            float(np.nanmedian(values)) if np.any(~np.isnan(values)) else 0.0
        )
    return CategoricalEncoder(
        categories=MappingProxyType(categories),
        continuous_medians=MappingProxyType(medians),
    )


def encode_features(
    frame: pd.DataFrame,
    *,
    continuous_columns: Sequence[str],
    encoder: CategoricalEncoder,
) -> np.ndarray:
    """Build a float64 design matrix from continuous and one-hot categorical columns.

    Continuous columns are median-filled; categorical columns are one-hot encoded
    against the encoder's fixed categories, so an unseen or missing level maps to an
    all-zero block and train and recipient matrices share an identical column layout.

    Args:
        frame: Frame to encode.
        continuous_columns: Numeric predictor columns.
        encoder: Encoder carrying the fixed categorical levels.

    Returns:
        A `(len(frame), n_continuous + sum(n_categories))` float64 design matrix.
    """
    blocks = []
    if continuous_columns:
        blocks.append(
            encode_design_matrix(
                frame, continuous_columns, medians=encoder.continuous_medians
            )
        )
    for column, levels in encoder.categories.items():
        as_string = frame[column].astype(str)
        blocks.extend(
            (as_string == level).to_numpy(dtype="float64").reshape(-1, 1)
            for level in levels
        )
    if not blocks:
        return np.empty((len(frame), 0), dtype="float64")
    return np.hstack(blocks).astype("float64")


def lagged_wealth(
    wealth_panel: pd.DataFrame,
    *,
    value_columns: Sequence[str],
    wave_gap: int = 5,
) -> pd.DataFrame:
    """Shift prior-wave wealth values forward by one wealth-module cycle.

    Each person's wealth at `survey_year` becomes a `lagged_`-prefixed predictor for
    that person at `survey_year + wave_gap` (the next wealth wave). The earliest wave
    has no predecessor and so is absent from the result; assembly left-joins this onto
    the feature matrix, leaving new entrants' lags as NA.

    Args:
        wealth_panel: Columns `p_id`, `survey_year`, and every name in
            `value_columns`.
        value_columns: Prior-wave wealth columns to carry forward.
        wave_gap: Years between wealth waves (the SOEP module runs every five years).

    Returns:
        Columns `p_id`, `survey_year` (the wave the lag applies to), and
        `lagged_<column>` for each value column.

    Raises:
        ValueError: If a required column is missing from `wealth_panel`.
    """
    required = ("p_id", "survey_year", *value_columns)
    missing = [column for column in required if column not in wealth_panel.columns]
    if missing:
        msg = f"wealth_panel is missing required columns: {missing}"
        raise ValueError(msg)
    lagged = wealth_panel[list(required)].copy()
    lagged["survey_year"] = lagged["survey_year"] + wave_gap
    return lagged.rename(
        columns={column: f"lagged_{column}" for column in value_columns}
    )


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
