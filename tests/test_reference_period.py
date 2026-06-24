"""Reference-period helper: turn `survey_year` into `ryear` / `rmonth`.

A SOEP value's reference period — survey year, previous calendar year, or the month
before the interview — is per-variable metadata, not encoded in the name. These tests
pin the `survey_year` -> `ryear`/`rmonth` mapping, including the January->December
rollover for previous-month references.
"""

import pandas as pd
import pytest

from soep_preparation.utilities.reference_period import (
    ReferencePeriod,
    add_reference_columns,
)


def test_current_reference_year_equals_survey_year():
    """A current-status variable references its own survey year."""
    result = add_reference_columns(
        survey_year=pd.Series([2020, 2021]), reference=ReferencePeriod.CURRENT
    )
    pd.testing.assert_series_equal(
        result["ryear"], pd.Series([2020, 2021], name="ryear", dtype="int64[pyarrow]")
    )


def test_previous_year_reference_year_is_survey_year_minus_one():
    """An annual flow references the previous calendar year."""
    result = add_reference_columns(
        survey_year=pd.Series([2020, 2021]), reference=ReferencePeriod.PREVIOUS_YEAR
    )
    pd.testing.assert_series_equal(
        result["ryear"], pd.Series([2019, 2020], name="ryear", dtype="int64[pyarrow]")
    )


def test_previous_month_within_year_keeps_survey_year():
    """A March interview's previous month is February of the same year."""
    result = add_reference_columns(
        survey_year=pd.Series([2020]),
        reference=ReferencePeriod.PREVIOUS_MONTH,
        interview_month=pd.Series([3]),
    )
    assert result["ryear"].tolist() == [2020]
    assert result["rmonth"].tolist() == [2]


def test_previous_month_rolls_january_back_to_december():
    """A January interview's previous month is December of the prior year."""
    result = add_reference_columns(
        survey_year=pd.Series([2020]),
        reference=ReferencePeriod.PREVIOUS_MONTH,
        interview_month=pd.Series([1]),
    )
    assert result["ryear"].tolist() == [2019]
    assert result["rmonth"].tolist() == [12]


def test_time_invariant_has_no_reference_period():
    """A time-invariant variable (e.g. birth year) has no reference year or month."""
    result = add_reference_columns(
        survey_year=pd.Series([2020]), reference=ReferencePeriod.TIME_INVARIANT
    )
    assert result["ryear"].isna().all()
    assert result["rmonth"].isna().all()


def test_previous_month_without_interview_month_raises():
    """A previous-month reference needs the interview month to resolve."""
    with pytest.raises(ValueError, match="interview_month"):
        add_reference_columns(
            survey_year=pd.Series([2020]), reference=ReferencePeriod.PREVIOUS_MONTH
        )


def test_string_reference_is_accepted():
    """The reference may be passed as its string value, not only the enum."""
    result = add_reference_columns(
        survey_year=pd.Series([2020]), reference="previous_year"
    )
    assert result["ryear"].tolist() == [2019]


def test_unknown_reference_raises():
    """An unrecognized reference value fails loudly."""
    with pytest.raises(ValueError, match="reference"):
        add_reference_columns(survey_year=pd.Series([2020]), reference="last_decade")
