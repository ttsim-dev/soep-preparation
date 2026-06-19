import math

import numpy as np
import pandas as pd
import pytest

from soep_preparation.wealth_imputation.aggregate import (
    household_component_sum,
    punr_residual,
)


def _panel() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "p_id": [1, 2, 3, 1],
            "hh_id": [1, 1, 2, 1],
            "survey_year": [2017, 2017, 2017, 2012],
            "component": ["financial_assets"] * 4,
            "person_component_value": [100.0, 50.0, 200.0, 70.0],
        }
    )


def _totals(value: float, component: str = "financial_assets") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "hh_id": [1],
            "survey_year": [2017],
            "component": [component],
            "household_total": [value],
        }
    )


def _members(value: float, component: str = "financial_assets") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "hh_id": [1],
            "survey_year": [2017],
            "component": [component],
            "member_sum": [value],
        }
    )


def test_household_component_sum_groups_by_household_year_and_component() -> None:
    out = household_component_sum(_panel())
    keyed = {
        (hh_id, survey_year, component): member_sum
        for hh_id, survey_year, component, member_sum in zip(
            out["hh_id"],
            out["survey_year"],
            out["component"],
            out["member_sum"],
            strict=True,
        )
    }
    assert keyed == {
        (1, 2017, "financial_assets"): 150.0,
        (2, 2017, "financial_assets"): 200.0,
        (1, 2012, "financial_assets"): 70.0,
    }


def test_household_component_sum_keeps_distinct_components_separate() -> None:
    panel = pd.DataFrame(
        {
            "p_id": [1, 1],
            "hh_id": [1, 1],
            "survey_year": [2017, 2017],
            "component": ["financial_assets", "consumer_debt"],
            "person_component_value": [100.0, 30.0],
        }
    )
    out = household_component_sum(panel)
    by_component = dict(zip(out["component"], out["member_sum"], strict=True))
    assert by_component == {"financial_assets": 100.0, "consumer_debt": 30.0}


def test_household_component_sum_returns_float64_member_sum() -> None:
    assert household_component_sum(_panel())["member_sum"].dtype == np.float64


def test_household_component_sum_computes_in_float64_not_float32() -> None:
    # In float32 this sum collapses to 0.0; float64 (before the sum) preserves 1.0.
    panel = pd.DataFrame(
        {
            "p_id": [1, 2, 3],
            "hh_id": [1, 1, 1],
            "survey_year": [2017, 2017, 2017],
            "component": ["financial_assets"] * 3,
            "person_component_value": pd.Series([1e8, 1.0, -1e8], dtype="float32"),
        }
    )
    out = household_component_sum(panel)
    assert math.isclose(out["member_sum"].iloc[0], 1.0)


def test_household_component_sum_sums_numeric_strings_not_concatenates() -> None:
    panel = pd.DataFrame(
        {
            "p_id": [1, 2],
            "hh_id": [1, 1],
            "survey_year": [2017, 2017],
            "component": ["financial_assets", "financial_assets"],
            "person_component_value": ["100", "50"],
        }
    )
    out = household_component_sum(panel)
    assert math.isclose(out["member_sum"].iloc[0], 150.0)


def test_household_component_sum_rejects_non_numeric_value() -> None:
    panel = _panel()
    panel["person_component_value"] = panel["person_component_value"].astype("object")
    panel.loc[0, "person_component_value"] = "abc"
    with pytest.raises(ValueError, match="Unable to parse"):
        household_component_sum(panel)


def test_household_component_sum_rejects_non_finite_value() -> None:
    panel = _panel()
    panel.loc[0, "person_component_value"] = np.inf
    with pytest.raises(ValueError, match="finite"):
        household_component_sum(panel)


def test_household_component_sum_rejects_duplicate_person_component_rows() -> None:
    panel = _panel()
    with pytest.raises(ValueError, match="duplicate"):
        household_component_sum(pd.concat([panel, panel.iloc[[0]]], ignore_index=True))


def test_household_component_sum_rejects_missing_person_value() -> None:
    panel = _panel()
    panel.loc[0, "person_component_value"] = pd.NA
    with pytest.raises(ValueError, match="missing person_component_value"):
        household_component_sum(panel)


def test_household_component_sum_rejects_missing_grouping_key() -> None:
    panel = _panel()
    panel.loc[0, "hh_id"] = pd.NA
    with pytest.raises(ValueError, match="missing values in key column 'hh_id'"):
        household_component_sum(panel)


def test_household_component_sum_rejects_missing_required_column() -> None:
    with pytest.raises(ValueError, match="missing required columns"):
        household_component_sum(_panel().drop(columns=["p_id"]))


def test_household_component_sum_rejects_non_canonical_component() -> None:
    panel = _panel()
    panel.loc[0, "component"] = "not_a_component"
    with pytest.raises(ValueError, match="non-canonical"):
        household_component_sum(panel)


def test_punr_residual_is_zero_when_member_sum_equals_household_total() -> None:
    out = punr_residual(_totals(150.0), _members(150.0))
    assert math.isclose(out["punr_residual"].iloc[0], 0.0)


def test_punr_residual_is_positive_when_household_exceeds_member_sum() -> None:
    out = punr_residual(_totals(150.0), _members(100.0))
    assert math.isclose(out["punr_residual"].iloc[0], 50.0)


def test_punr_residual_for_debt_uses_positive_magnitudes() -> None:
    # Debt is carried as positive magnitudes: 200 household - 150 members = +50 PUNR.
    out = punr_residual(
        _totals(200.0, "consumer_debt"), _members(150.0, "consumer_debt")
    )
    assert math.isclose(out["punr_residual"].iloc[0], 50.0)


def test_punr_residual_returns_float64() -> None:
    out = punr_residual(_totals(150.0), _members(100.0))
    assert out["punr_residual"].dtype == np.float64


def test_punr_residual_rejects_keys_present_on_only_one_side() -> None:
    members = _members(100.0).assign(hh_id=[2])
    with pytest.raises(ValueError, match="identical"):
        punr_residual(_totals(150.0), members)


def test_punr_residual_rejects_duplicate_keys() -> None:
    totals = pd.concat([_totals(150.0), _totals(150.0)], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate"):
        punr_residual(totals, _members(100.0))
