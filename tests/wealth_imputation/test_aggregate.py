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


def test_household_component_sum_returns_float64_member_sum() -> None:
    out = household_component_sum(_panel())
    assert out["member_sum"].dtype == np.float64


def test_household_component_sum_rejects_duplicate_person_component_rows() -> None:
    panel = _panel()
    with pytest.raises(ValueError, match="duplicate"):
        household_component_sum(pd.concat([panel, panel.iloc[[0]]], ignore_index=True))


def test_household_component_sum_rejects_missing_person_value() -> None:
    panel = _panel()
    panel.loc[0, "person_component_value"] = pd.NA
    with pytest.raises(ValueError, match="missing person_component_value"):
        household_component_sum(panel)


def test_household_component_sum_rejects_missing_required_column() -> None:
    with pytest.raises(ValueError, match="missing required columns"):
        household_component_sum(_panel().drop(columns=["p_id"]))


def test_punr_residual_is_zero_when_member_sum_equals_household_total() -> None:
    total = pd.Series([150.0, 200.0])
    member_sum = pd.Series([150.0, 200.0])
    pd.testing.assert_series_equal(
        punr_residual(total, member_sum), pd.Series([0.0, 0.0]), check_names=False
    )


def test_punr_residual_is_positive_when_household_exceeds_member_sum() -> None:
    total = pd.Series([150.0])
    member_sum = pd.Series([100.0])
    assert math.isclose(punr_residual(total, member_sum).iloc[0], 50.0)


def test_punr_residual_for_debt_uses_positive_magnitudes() -> None:
    # Debt is carried as positive magnitudes: 200 household - 150 members = +50 PUNR.
    household_debt = pd.Series([200.0])
    member_debt = pd.Series([150.0])
    assert math.isclose(punr_residual(household_debt, member_debt).iloc[0], 50.0)


def test_punr_residual_rejects_mismatched_index() -> None:
    total = pd.Series([150.0], index=[0])
    member_sum = pd.Series([100.0], index=[1])
    with pytest.raises(ValueError, match="identical index"):
        punr_residual(total, member_sum)
