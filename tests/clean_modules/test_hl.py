"""Tests for hl-module cleaning helpers."""

import pandas as pd

from soep_preparation.clean_modules.hl import _grundsicherung_im_alter_received


def test_receipt_true_for_positive_amount():
    """A positive Grundsicherung-im-Alter amount counts as receipt."""
    amount = pd.Series([500.0, 1_200.0], dtype="float[pyarrow]")
    out = _grundsicherung_im_alter_received(amount)
    assert out.tolist() == [True, True]


def test_receipt_false_for_zero_amount():
    """A zero amount (incl. not-applicable cleaned to 0) is non-receipt."""
    amount = pd.Series([0.0, 0.0], dtype="float[pyarrow]")
    out = _grundsicherung_im_alter_received(amount)
    assert out.tolist() == [False, False]


def test_receipt_missing_for_missing_amount():
    """A genuinely missing amount yields a missing receipt flag, not False."""
    amount = pd.Series([pd.NA], dtype="float[pyarrow]")
    out = _grundsicherung_im_alter_received(amount)
    assert out.isna().tolist() == [True]


def test_receipt_dtype_is_pyarrow_bool():
    """The receipt flag uses the project-wide `bool[pyarrow]` dtype."""
    amount = pd.Series([0.0, 500.0], dtype="float[pyarrow]")
    out = _grundsicherung_im_alter_received(amount)
    assert out.dtype == "bool[pyarrow]"
