import math

import numpy as np
import pandas as pd
import pytest

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


def test_resolve_person_amount_returns_float64():
    result = resolve_person_amount(pd.Series([200_000.0]), pd.Series([0.5]))
    assert result.dtype == np.float64


def test_resolve_person_amount_multiplies_in_float64_after_promotion():
    joint = pd.Series([3.0], dtype="float32")
    share = pd.Series([0.1], dtype="float32")
    result = resolve_person_amount(joint, share)
    expected = np.float64(np.float32(3.0)) * np.float64(np.float32(0.1))
    assert result.dtype == np.float64
    assert math.isclose(result.iloc[0], expected)


def test_resolve_person_amount_rejects_mismatched_index():
    joint = pd.Series([200_000.0], index=[0])
    share = pd.Series([0.5], index=[1])
    with pytest.raises(ValueError, match="identical index"):
        resolve_person_amount(joint, share)


@pytest.mark.parametrize("bad_share", [-0.1, 1.5])
def test_resolve_person_amount_rejects_share_outside_unit_interval(bad_share: float):
    with pytest.raises(ValueError, match=r"\[0, 1\]"):
        resolve_person_amount(pd.Series([200_000.0]), pd.Series([bad_share]))


def test_resolve_person_amount_rejects_non_finite_amount():
    with pytest.raises(ValueError, match="finite"):
        resolve_person_amount(pd.Series([np.inf]), pd.Series([0.5]))
