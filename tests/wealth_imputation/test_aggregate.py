import pandas as pd

from soep_preparation.wealth_imputation.aggregate import (
    household_component_sum,
    punr_residual,
)


def test_household_component_sum_sums_members_within_household_component() -> None:
    panel = pd.DataFrame(
        {
            "hh_id": [1, 1, 2],
            "survey_year": [2017, 2017, 2017],
            "component": ["financial_assets", "financial_assets", "financial_assets"],
            "person_component_value": [100.0, 50.0, 200.0],
        }
    )
    out = household_component_sum(panel)
    sums = dict(zip(out["hh_id"], out["member_sum"], strict=True))
    assert sums == {1: 150.0, 2: 200.0}


def test_punr_residual_is_zero_when_member_sum_equals_household_total() -> None:
    total = pd.Series([150.0, 200.0])
    member_sum = pd.Series([150.0, 200.0])
    pd.testing.assert_series_equal(
        punr_residual(total, member_sum),
        pd.Series([0.0, 0.0]),
        check_names=False,
    )


def test_punr_residual_is_positive_when_household_exceeds_member_sum() -> None:
    total = pd.Series([150.0])
    member_sum = pd.Series([100.0])
    expected_residual = 50.0
    assert punr_residual(total, member_sum).iloc[0] == expected_residual
