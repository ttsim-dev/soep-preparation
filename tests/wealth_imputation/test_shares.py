import pandas as pd

from soep_preparation.wealth_imputation.shares import resolve_person_amount


def test_resolve_person_amount_applies_share_once():
    joint = pd.Series([200_000.0, 200_000.0])
    share = pd.Series([0.5, 1.0])
    result = resolve_person_amount(joint, share)
    pd.testing.assert_series_equal(
        result, pd.Series([100_000.0, 200_000.0]), check_names=False
    )


def test_resolve_person_amount_missing_share_yields_na():
    joint = pd.Series([200_000.0])
    share = pd.Series([pd.NA], dtype="Float64")
    result = resolve_person_amount(joint, share)
    assert pd.isna(result.iloc[0])
