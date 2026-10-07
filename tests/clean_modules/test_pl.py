"""Tests for the pl cleaning module."""

import pandas as pd
import pytest

from soep_preparation.clean_modules.pl import (
    _EMPLOYMENT_ENDED_REASON_HARMONIZED_EN,
    _arbeitslosengeld_received_last_month,
)
from soep_preparation.utilities.data_manipulator import (
    object_to_str_categorical,
    translate_categories,
)


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


@pytest.mark.parametrize(
    ("soep_label", "expected"),
    [
        ("[1] Wegen Betriebsstilllegung", "Plant closure"),
        ("[2] Durch eigene Kuendigung", "Own resignation"),
        ("[3] Durch Kuendigung des Arbeitgebers", "Dismissal by employer"),
        ("[4] Durch Aufloesungsvertrag", "Termination agreement"),
        ("[5] Befristete Beschaeftigung war beendet", "Fixed-term employment ended"),
        ("[6] Erreichen der Altersgrenze", "Reached age limit"),
        (
            "[7] Beurlaubung/Mutterschutz/Elternzeit",
            "Leave of absence / maternity / parental leave",
        ),
        ("[8] Aufgabe der selbstaendigen Taetigkeit", "Gave up self-employment"),
        ("[9] Ende Ausbildung", "Vocational training ended"),
        ("[10] Versetzung auf eigenen Wunsch", "Transfer at own request"),
        ("[11] Versetzung durch Betrieb", "Transfer by employer"),
        ("[12] Rente", "Retirement"),
        ("[13] Sonstife Gruende f-Stellenausscheidung", "Other reasons"),
        ("[14] Mehrfachnennung", "Multiple reasons"),
        (
            "[15] Weil Arbeitserlaubnis nicht verlängert wurde",
            "Work permit not renewed",
        ),
    ],
)
def test_employment_ended_reason_harmonized_translates_soep_label(
    soep_label: str, expected: str
) -> None:
    """Each SOEP `plb0304_h` label maps to its English category."""
    result = translate_categories(
        object_to_str_categorical(pd.Series([soep_label], dtype="object")),
        _EMPLOYMENT_ENDED_REASON_HARMONIZED_EN,
    )
    assert result.iloc[0] == expected
