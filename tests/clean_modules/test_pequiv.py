"""Tests for the pequiv cleaning module."""

from types import MappingProxyType

import pandas as pd
import pytest

from soep_preparation.clean_modules.pequiv import (
    _calculate_dependent_employment_income,
    clean,
)
from soep_preparation.config import SRC
from soep_preparation.utilities.general import get_relevant_column_names

NOT_APPLICABLE = "[-2] trifft nicht zu"
IDENTIFIERS = MappingProxyType({"hid": 1, "pid": 101, "cid": 1, "syear": 2017})

INCOME_VARIABLE_TO_RAW_CODE = MappingProxyType(
    {
        "income_before_tax_y_hh": "i11101",
        "income_after_tax_y_hh": "i11102",
        "income_from_interest_dividends_y_hh": "divdy",
        "mutterschaftsgeld_received_y": "imaty",
        "arbeitslosengeld_y": "iunby",
        "arbeitslosenhilfe_y": "iunay",
        "grundsicherung_y": "isuby",
        "private_transfers_received_y": "ielse",
        "unterhalt_received_y": "ialim",
        "kindesunterhalt_received_y": "ichsu",
        "ehegattenunterhalt_received_y": "ispou",
        "unterhaltsvorschuss_received_y": "iachm",
        "bafög_y": "istuy",
        "gesetzliche_rente_y": "igrv1",
        "gesetzliche_rente_survivor_y": "igrv2",
        "knappschaftliche_rente_y": "ismp1",
        "knappschaftliche_rente_survivor_y": "ismp2",
        "alterssicherung_landwirte_y": "iagr1",
        "alterssicherung_landwirte_survivor_y": "iagr2",
        "kriegsopferversorgung_rente_y": "iwar1",
        "kriegsopferversorgung_rente_survivor_y": "iwar2",
        "beamtenpension_y": "iciv1",
        "beamtenpension_survivor_y": "iciv2",
        "beamtenpension_supplementary_y": "ivbl1",
        "beamtenpension_supplementary_survivor_y": "ivbl2",
        "vorruhestandsgeld_y": "ieret",
        "betriebliche_altersversorgung_y": "icom1",
        "betriebliche_altersversorgung_survivor_y": "icom2",
        "private_altersvorsorge_y": "iprv1",
        "private_altersvorsorge_survivor_y": "iprv2",
        "berufsständische_altersvorsorge_y": "ilib1",
        "riester_rente_y": "irie1",
        "riester_rente_survivor_y": "irie2",
        "gesetzliche_unfallversicherung_rente_y": "iguv1",
        "other_pension_y": "ison1",
        "other_pension_survivor_y": "ison2",
        "earnings_from_work_y": "i11110",
        "earnings_from_first_job_y": "ijob1",
        "earnings_from_second_job_y": "ijob2",
        "earnings_from_self_employment_y": "iself",
        "thirteenth_monthly_salary_y": "i13ly",
        "fourteenth_monthly_salary_y": "i14ly",
        "christmas_bonus_y": "ixmas",
        "holiday_bonus_y": "iholy",
        "profit_sharing_y": "igray",
        "other_bonuses_y": "iothy",
    }
)


def _make_series(*values: float | None) -> pd.Series:
    return pd.Series(values, dtype="float64[pyarrow]")


@pytest.fixture(scope="module")
def cleaned_not_applicable_row() -> pd.DataFrame:
    """Clean one person-year whose every income variable reads `-2`.

    The income columns get the labelled form the pipeline delivers. Everything
    else gets a plain numeric `-2`, because the count columns are read with
    `apply_smallest_int_dtype`, which cannot parse a labelled string.
    """
    income_codes = set(INCOME_VARIABLE_TO_RAW_CODE.values())
    columns = get_relevant_column_names(SRC / "clean_modules" / "pequiv.py")

    def value_for(column: str) -> object:
        if column in IDENTIFIERS:
            return IDENTIFIERS[column]
        return NOT_APPLICABLE if column in income_codes else -2

    raw_data = pd.DataFrame(
        {column: [value_for(column)] for column in [*IDENTIFIERS, *columns]},
        dtype="object",
    )
    return clean(raw_data)


@pytest.mark.parametrize("variable", list(INCOME_VARIABLE_TO_RAW_CODE))
def test_not_applicable_income_is_missing_rather_than_zero(
    cleaned_not_applicable_row: pd.DataFrame, variable: str
) -> None:
    """A `-2` pequiv income code cleans to a missing value, never to zero."""
    assert cleaned_not_applicable_row[variable].isna().all()


def test_dependent_employment_income_sums_wage_and_bonus_components() -> None:
    """Dependent-employment income adds up the non-self-employed pay components."""
    expected = _make_series(36500.0)

    actual = _calculate_dependent_employment_income(
        earnings_from_first_job_y=_make_series(30000.0),
        earnings_from_second_job_y=_make_series(2000.0),
        thirteenth_monthly_salary_y=_make_series(2500.0),
        fourteenth_monthly_salary_y=_make_series(0.0),
        christmas_bonus_y=_make_series(1000.0),
        holiday_bonus_y=_make_series(500.0),
        profit_sharing_y=_make_series(300.0),
        other_bonuses_y=_make_series(200.0),
    )

    pd.testing.assert_series_equal(actual, expected)


def test_dependent_employment_income_ignores_missing_components() -> None:
    """A missing component does not wipe out the reported ones."""
    expected = _make_series(31000.0)

    actual = _calculate_dependent_employment_income(
        earnings_from_first_job_y=_make_series(30000.0),
        earnings_from_second_job_y=_make_series(None),
        thirteenth_monthly_salary_y=_make_series(None),
        fourteenth_monthly_salary_y=_make_series(None),
        christmas_bonus_y=_make_series(1000.0),
        holiday_bonus_y=_make_series(None),
        profit_sharing_y=_make_series(None),
        other_bonuses_y=_make_series(None),
    )

    pd.testing.assert_series_equal(actual, expected)
