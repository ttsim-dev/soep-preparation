"""Tests for the pgen cleaning module."""

import pandas as pd
import pytest

from soep_preparation.clean_modules.pgen import _fail_if_expected_categories_absent


def test_fail_if_expected_categories_absent_raises_on_missing_label() -> None:
    """A label absent from the data's categories fails loudly."""
    series = pd.Series(["a", "b"], dtype="category")
    with pytest.raises(ValueError, match="absent from the data"):
        _fail_if_expected_categories_absent(series=series, expected=["a", "c"])


def test_fail_if_expected_categories_absent_passes_when_all_present() -> None:
    """No error when every expected label is among the categories."""
    series = pd.Series(["a", "b"], dtype="category")
    _fail_if_expected_categories_absent(series=series, expected=["a", "b"])
