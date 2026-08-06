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

# Every monetary pequiv variable, with the raw code it is cleaned from.
MONETARY_VARIABLE_TO_RAW_CODE = MappingProxyType(
    {
        "income_before_tax_y_hh": "i11101",
        "income_after_tax_y_hh": "i11102",
        "income_from_rental_leasing_y_hh": "renty",
        "income_from_interest_dividends_y_hh": "divdy",
        "kindergeld_y_hh_pequiv": "chspt",
        "mutterschaftsgeld_received_y": "imaty",
        "betreuungsgeld_y_hh": "chsub",
        "kinderzuschlag_y_hh_pequiv": "adchb",
        "wohngeld_y_hh_pequiv": "house",
        "arbeitslosengeld_y": "iunby",
        "arbeitslosenhilfe_y": "iunay",
        "arbeitslosengeld_2_y_hh_pequiv": "alg2",
        "sozialhilfe_general_y_hh": "subst",
        "sozialhilfe_other_y_hh": "sphlp",
        "grundsicherung_y": "isuby",
        "grundsicherung_im_alter_y_hh": "ssold",
        "pflegegeld_y_hh": "nursh",
        "eigenheimzulage_y_hh": "hsup",
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
        "berufsständische_altersvorsorge_survivor_y": "ilib2",
        "riester_rente_y": "irie1",
        "riester_rente_survivor_y": "irie2",
        "gesetzliche_unfallversicherung_rente_y": "iguv1",
        "gesetzliche_unfallversicherung_rente_survivor_y": "iguv2",
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
        "operation_maintenance_costs_y_hh": "opery",
    }
)


def _raw_data_holding(variable: str, value: object) -> pd.DataFrame:
    """Build one raw person-year in which `variable` is read from `value`.

    Every other raw column holds -2, which parses under all cleaning functions the
    module applies, including the ones that cannot read labelled strings.
    """
    columns = get_relevant_column_names(SRC / "clean_modules" / "pequiv.py")
    raw_data = pd.DataFrame({column: [-2] for column in columns}, dtype="object")
    raw_code = MONETARY_VARIABLE_TO_RAW_CODE[variable]
    raw_data[raw_code] = pd.Series([value], dtype="object")
    return raw_data


@pytest.mark.parametrize("variable", MONETARY_VARIABLE_TO_RAW_CODE)
@pytest.mark.parametrize(
    "missing_code",
    [
        -1,
        -2,
        -3,
        -4,
        -5,
        -6,
        -7,
        -8,
        "[-1] keine Angabe",
        "[-2] trifft nicht zu",
        "[-3] unplausibler Wert",
        "[-4] Unzulaessige Mehrfachantwort",
        "[-5] in Fragebogenversion nicht enthalten",
        "[-6] Fragebogenversion mit geaenderter Filterfuehrung",
        "[-7] nur in weniger detaillierter Fassung vorhanden",
        "[-8] Frage in diesem Jahr nicht erhoben",
    ],
)
def test_missing_code_cleans_to_missing(variable: str, missing_code: object) -> None:
    """No negative pequiv code survives cleaning, whether numeric or labelled."""
    raw_data = _raw_data_holding(variable=variable, value=missing_code)

    expected = pd.Series([None], dtype="float64[pyarrow]")
    actual = clean(raw_data)[variable]

    pd.testing.assert_series_equal(actual, expected, check_names=False)


@pytest.mark.parametrize("variable", MONETARY_VARIABLE_TO_RAW_CODE)
def test_zero_stays_zero(variable: str) -> None:
    """A genuine zero means "no income of this kind" and must not become missing."""
    raw_data = _raw_data_holding(variable=variable, value=0.0)

    expected = pd.Series([0.0], dtype="float64[pyarrow]")
    actual = clean(raw_data)[variable]

    pd.testing.assert_series_equal(actual, expected, check_names=False)


@pytest.mark.parametrize("variable", MONETARY_VARIABLE_TO_RAW_CODE)
def test_reported_amount_survives_cleaning(variable: str) -> None:
    """A reported amount is kept as is."""
    raw_data = _raw_data_holding(variable=variable, value=1234.5)

    expected = pd.Series([1234.5], dtype="float64[pyarrow]")
    actual = clean(raw_data)[variable]

    pd.testing.assert_series_equal(actual, expected, check_names=False)


def _make_series(*values: float | None) -> pd.Series:
    return pd.Series(values, dtype="float64[pyarrow]")


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
