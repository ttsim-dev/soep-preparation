"""Behavior of the wealth-model feature registry."""

import pandas as pd
import pytest

from soep_preparation.wealth_imputation.features import (
    FEATURE_SPECS,
    assemble_feature_matrix,
    fail_if_features_missing,
    required_columns_by_module,
)


def _modules_with_two_persons_one_household() -> dict[str, pd.DataFrame]:
    """Minimal cleaned modules: two persons in one owner-occupied household."""
    columns = required_columns_by_module()
    frames: dict[str, pd.DataFrame] = {}
    for module, cols in columns.items():
        if "p_id" in cols:  # person-level module
            frame = pd.DataFrame({col: [0, 0] for col in cols})
            frame["p_id"] = [1, 2]
            frame["hh_id"] = [10, 10]
            frame["survey_year"] = [2017, 2017]
        else:  # household-level module
            frame = pd.DataFrame({col: [0] for col in cols})
            frame["hh_id"] = [10]
            frame["survey_year"] = [2017]
        frames[module] = frame
    frames["pequiv"]["age"] = [40, 38]
    frames["hgen"]["rented_or_owned"] = ["owned"]
    return frames


def test_assemble_feature_matrix_joins_persons_and_broadcasts_household_features():
    """Each person row carries its own and its household's predictors."""
    modules = _modules_with_two_persons_one_household()
    expected_persons = len(modules["pequiv"])
    result = assemble_feature_matrix(modules)
    assert len(result) == expected_persons
    by_person = result.set_index("p_id")
    expected_age = modules["pequiv"].set_index("p_id").loc[1, "age"]
    assert by_person.loc[1, "age"] == expected_age
    # Household-level predictor is broadcast to every member.
    assert by_person.loc[1, "rented_or_owned"] == "owned"
    assert by_person.loc[2, "rented_or_owned"] == "owned"


def test_assemble_feature_matrix_exposes_every_registered_feature():
    """The design matrix carries each registered predictor column."""
    result = assemble_feature_matrix(_modules_with_two_persons_one_household())
    expected = {spec.column for spec in FEATURE_SPECS}
    assert expected <= set(result.columns)


def test_required_columns_by_module_includes_keys_and_feature_columns():
    """Each module lists its level keys plus the features drawn from it."""
    by_module = required_columns_by_module()
    assert by_module["hgen"] == (
        "hh_id",
        "survey_year",
        "rented_or_owned",
        "living_space_hh",
        "building_year_hh_min",
    )
    assert {"p_id", "hh_id", "survey_year", "age", "gender"} <= set(by_module["pequiv"])


def test_fail_if_features_missing_passes_when_all_columns_present():
    """A complete set of modules with every required column does not raise."""
    modules = {
        module: pd.DataFrame(columns=list(columns))
        for module, columns in required_columns_by_module().items()
    }
    fail_if_features_missing(modules)  # no raise


def test_fail_if_features_missing_raises_on_missing_column():
    """A module missing a registered feature column fails closed."""
    modules = {
        module: pd.DataFrame(columns=list(columns))
        for module, columns in required_columns_by_module().items()
    }
    modules["hgen"] = modules["hgen"].drop(columns=["rented_or_owned"])
    with pytest.raises(ValueError, match="rented_or_owned"):
        fail_if_features_missing(modules)


def test_fail_if_features_missing_raises_on_absent_module():
    """An absent source module fails closed."""
    modules = {
        module: pd.DataFrame(columns=list(columns))
        for module, columns in required_columns_by_module().items()
        if module != "ppathl"
    }
    with pytest.raises(ValueError, match="ppathl"):
        fail_if_features_missing(modules)


def test_feature_specs_homeownership_is_a_household_predictor():
    """Homeownership is registered as a household-level predictor."""
    homeownership = next(
        spec for spec in FEATURE_SPECS if spec.column == "rented_or_owned"
    )
    assert homeownership.level == "household"
