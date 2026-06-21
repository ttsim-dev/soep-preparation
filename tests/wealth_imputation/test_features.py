"""Behavior of the wealth-model feature registry."""

import pandas as pd
import pytest

from soep_preparation.wealth_imputation.features import (
    FEATURE_SPECS,
    fail_if_features_missing,
    required_columns_by_module,
)


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
