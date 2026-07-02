"""Tests for the pl cleaning module."""

import pandas as pd

from soep_preparation.clean_modules.pl import _arbeitslosengeld_received_last_month


def test_arbeitslosengeld_last_month_prefers_v1_then_fills_from_v2() -> None:
    """The composite takes the all-sample version where present, else the M3-M5 one."""
    all_samples = pd.Series(["[1] Ja", -2, -2], dtype="object")
    m3_to_m5_sample = pd.Series([-2, "[1] Ja", 2], dtype="object")
    result = _arbeitslosengeld_received_last_month(
        all_samples=all_samples, m3_to_m5_sample=m3_to_m5_sample
    )
    assert list(result) == [True, True, False]


def test_arbeitslosengeld_last_month_has_ordered_bool_pyarrow_categories() -> None:
    """The composite carries ordered bool[pyarrow] {True, False} categories."""
    all_samples = pd.Series(["[1] Ja", -2], dtype="object")
    m3_to_m5_sample = pd.Series([-2, 2], dtype="object")
    result = _arbeitslosengeld_received_last_month(
        all_samples=all_samples, m3_to_m5_sample=m3_to_m5_sample
    )
    assert list(result.cat.categories) == [True, False]
