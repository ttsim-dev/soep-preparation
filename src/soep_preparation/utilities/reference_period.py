"""Turn `survey_year` into reference year/month columns (`ryear` / `rmonth`).

A SOEP value's reference period — the survey year, the previous calendar year, or the
month before the interview — is per-variable metadata, not encoded in the variable name.
This converts `survey_year` (the interview wave `t`) into the year and month the value
actually describes, with the January->December rollover for previous-month references.
"""

from enum import StrEnum

import pandas as pd


class ReferencePeriod(StrEnum):
    """The period a SOEP value describes, relative to the interview wave `t`."""

    CURRENT = "current"
    """The survey year itself (point-in-time / current-status questions)."""

    PREVIOUS_YEAR = "previous_year"
    """The previous calendar year (annual flows, the `pkal` calendar, month counts)."""

    PREVIOUS_MONTH = "previous_month"
    """The month before the interview ("current" monthly amounts, e.g. `pglabnet`)."""

    TIME_INVARIANT = "time_invariant"
    """No reference period (e.g. birth year, parents)."""


_INT = "int64[pyarrow]"


def add_reference_columns(
    survey_year: pd.Series,
    reference: ReferencePeriod | str,
    interview_month: pd.Series | None = None,
) -> pd.DataFrame:
    """Build `ryear` / `rmonth` from `survey_year` for one reference period.

    Args:
        survey_year: The interview wave `t` for each observation.
        reference: The variable's reference period, as a `ReferencePeriod` or its
            string value.
        interview_month: Month of the interview, required for `previous_month`.

    Returns:
        A frame with nullable-integer `ryear` and `rmonth` columns aligned to
        `survey_year`'s index. `rmonth` is `pd.NA` for every reference except
        `previous_month`; both are `pd.NA` for `time_invariant`. For
        `previous_month` rows whose `interview_month` is `pd.NA`, **both** `ryear`
        and `rmonth` are `pd.NA`: the reference year depends on the month (a January
        interview rolls back a year), so an unknown month leaves the year
        unidentifiable, and it is fail-closed rather than assumed to be the survey
        year.

    Raises:
        ValueError: If `reference` is unknown, `previous_month` is requested
            without `interview_month`, or an observed `interview_month` lies
            outside 1-12.
    """
    reference = _coerce_reference(reference)
    index = survey_year.index
    ryear = pd.Series(pd.NA, index=index, dtype=_INT)
    rmonth = pd.Series(pd.NA, index=index, dtype=_INT)

    if reference is ReferencePeriod.CURRENT:
        ryear = survey_year.astype(_INT)
    elif reference is ReferencePeriod.PREVIOUS_YEAR:
        ryear = (survey_year - 1).astype(_INT)
    elif reference is ReferencePeriod.PREVIOUS_MONTH:
        if interview_month is None:
            msg = (
                "A 'previous_month' reference needs `interview_month` to resolve "
                "`rmonth`."
            )
            raise ValueError(msg)
        _fail_if_interview_month_out_of_range(interview_month)
        # A January interview refers to December of the prior year; every other
        # month refers to the previous month in the survey year. With the month
        # unknown the year is unidentifiable, so both columns are left NA below.
        rolls_back = (interview_month == 1).fillna(value=False)
        ryear = (
            survey_year.where(~rolls_back, survey_year - 1)
            .where(interview_month.notna(), pd.NA)
            .astype(_INT)
        )
        rmonth = (
            interview_month.where(~rolls_back, 13)
            .sub(1)
            .where(interview_month.notna(), pd.NA)
            .astype(_INT)
        )
    # TIME_INVARIANT leaves both columns NA.

    return pd.DataFrame({"ryear": ryear, "rmonth": rmonth})


def _fail_if_interview_month_out_of_range(interview_month: pd.Series) -> None:
    observed = interview_month.dropna()
    if not observed.between(1, 12).all():
        msg = (
            "`interview_month` must be in 1-12 (or NA); got values outside that range."
        )
        raise ValueError(msg)


def _coerce_reference(reference: ReferencePeriod | str) -> ReferencePeriod:
    if isinstance(reference, ReferencePeriod):
        return reference
    try:
        return ReferencePeriod(reference)
    except ValueError:
        valid = sorted(member.value for member in ReferencePeriod)
        msg = f"Unknown reference {reference!r}; expected one of {valid}."
        raise ValueError(msg) from None
