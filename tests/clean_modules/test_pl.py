"""Tests for the pl cleaning module."""

import pandas as pd
import pytest

from soep_preparation.clean_modules.pl import (
    _arbeitslosengeld_received_last_month,
    _employment_ended_reason_harmonized,
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
    result = _employment_ended_reason_harmonized(
        pd.Series([soep_label], dtype="object")
    )
    assert result.iloc[0] == expected


@pytest.mark.parametrize(
    "missing_code",
    [-1, -2, -3, -4, -5, -8, "[-2] trifft nicht zu", "[-1] keine Angabe"],
)
def test_employment_ended_reason_harmonized_maps_missing_codes_to_na(
    missing_code: int | str,
) -> None:
    """SOEP missing-data codes become missing, not a category."""
    result = _employment_ended_reason_harmonized(
        pd.Series(["[2] Durch eigene Kuendigung", missing_code], dtype="object")
    )
    assert pd.isna(result.iloc[1])


def test_employment_ended_reason_harmonized_is_unordered_categorical() -> None:
    """The reasons carry no natural order, so the categorical is unordered."""
    result = _employment_ended_reason_harmonized(
        pd.Series(["[2] Durch eigene Kuendigung", -2], dtype="object")
    )
    assert result.cat.ordered is False


def test_employment_ended_reason_harmonized_fails_on_unknown_label() -> None:
    """A label absent from the translation map fails loudly."""
    with pytest.raises(ValueError, match="missing from the translation map"):
        _employment_ended_reason_harmonized(
            pd.Series(["[16] Neuer Grund"], dtype="object")
        )
