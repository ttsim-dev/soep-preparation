"""Tests for the ppathl cleaning module."""

import pandas as pd

from soep_preparation.clean_modules.ppathl import clean_year_of_death


def test_clean_year_of_death_keeps_known_years() -> None:
    """A recorded year of death passes through as an integer."""
    out = clean_year_of_death(pd.Series([2015, -2], dtype="object"))
    assert out.iloc[0] == 2015


def test_clean_year_of_death_marks_persons_not_known_dead_missing() -> None:
    """`-2` (not known to be deceased) becomes missing."""
    out = clean_year_of_death(pd.Series([2015, -2], dtype="object"))
    assert pd.isna(out.iloc[1])


def test_clean_year_of_death_marks_unknown_years_missing() -> None:
    """`-1` (deceased, year unknown) becomes missing."""
    out = clean_year_of_death(pd.Series([2015, -1], dtype="object"))
    assert pd.isna(out.iloc[1])
