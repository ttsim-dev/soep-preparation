"""Tests for the SOEP-to-GETTSIM mapping and its per-policy-date resolution.

The mapping must be honest: every non-`None` target names a real SOEP final variable,
the keys are GETTSIM qnames (`__`-qualified) rather than SOEP names, and the date
selector returns exactly the qnames in GETTSIM's policy environment at that date.
"""

import datetime
import itertools

import pytest

from soep_preparation.config import METADATA, POTENTIAL_INDEX_VARIABLES
from soep_preparation.gettsim_inputs.mapping import (
    BASE_MAPPING,
    GAP_NOTES,
    get_soep_to_gettsim,
    mapping_periods,
    period_for_date,
)

_VALID_SOEP_TARGETS = set(METADATA) | set(POTENTIAL_INDEX_VARIABLES)

_NON_NONE_ITEMS = [
    (gettsim_qname, soep_variable)
    for gettsim_qname, soep_variable in BASE_MAPPING.items()
    if soep_variable is not None
]


@pytest.mark.parametrize(("gettsim_qname", "soep_variable"), _NON_NONE_ITEMS)
def test_every_mapped_target_is_a_real_soep_variable(
    gettsim_qname: str,
    soep_variable: str,
) -> None:
    """Each non-`None` mapping value names a real SOEP final variable."""
    assert soep_variable in _VALID_SOEP_TARGETS, (
        f"{gettsim_qname} maps to {soep_variable!r}, which is not a SOEP variable."
    )


def test_living_space_maps_to_gettsim_living_area() -> None:
    """`wohnen__wohnfläche_hh` is supplied by SOEP `living_space_hh`."""
    assert BASE_MAPPING["wohnen__wohnfläche_hh"] == "living_space_hh"


def test_pension_entry_year_is_unmapped() -> None:
    """The statutory pension-entry year stays unmapped (SOEP has no claim event)."""
    assert BASE_MAPPING["sozialversicherung__rente__jahr_renteneintritt"] is None


def test_pension_entry_year_gap_is_documented() -> None:
    """The unmapped pension-entry year is recorded in `GAP_NOTES`."""
    assert "sozialversicherung__rente__jahr_renteneintritt" in GAP_NOTES


def test_maintenance_received_is_unmapped() -> None:
    """Generic maintenance received stays unmapped: SOEP only has child maintenance."""
    assert BASE_MAPPING["unterhalt__tatsächlich_erhaltener_betrag_m"] is None


def test_steuerklasse_has_no_soep_source() -> None:
    """`lohnsteuer__steuerklasse` has no SOEP source and stays unmapped."""
    assert BASE_MAPPING["lohnsteuer__steuerklasse"] is None


def test_ehepartner_maps_to_marriage_restricted_pointer() -> None:
    """The spouse pointer uses `ehepartner_p_id`, not the generic partner pointer."""
    assert BASE_MAPPING["familie__p_id_ehepartner"] == "ehepartner_p_id"


def test_all_keys_are_gettsim_qnames_not_soep_names() -> None:
    """GETTSIM-qname keys must not collide with SOEP final variable names."""
    assert set(BASE_MAPPING) & set(METADATA) == set()


def test_kindergeld_amount_is_a_non_root_candidate() -> None:
    """The computed node `kindergeld__betrag_m` is listed as an override candidate."""
    assert "kindergeld__betrag_m" in BASE_MAPPING


def test_kindergeld_amount_is_unmapped_with_documented_gap() -> None:
    """`kindergeld__betrag_m` stays `None` with a documented level mismatch."""
    assert BASE_MAPPING["kindergeld__betrag_m"] is None
    assert "kindergeld__betrag_m" in GAP_NOTES


def test_weiblich_is_in_scope_before_2018() -> None:
    """`weiblich` is a GETTSIM input through 2017 (Altersrente für Frauen)."""
    assert "weiblich" in get_soep_to_gettsim(datetime.date(2015, 1, 1))


def test_weiblich_is_out_of_scope_from_2018() -> None:
    """`weiblich` leaves GETTSIM's inputs once the women's pension expires in 2018."""
    assert "weiblich" not in get_soep_to_gettsim(datetime.date(2019, 1, 1))


def test_alg2_einstandspartner_is_in_scope_before_buergergeld() -> None:
    """The ALG II Einstandspartner pointer is in scope before Bürgergeld (2023)."""
    mapping = get_soep_to_gettsim(datetime.date(2015, 1, 1))
    assert mapping["arbeitslosengeld_2__p_id_einstandspartner"] == "partner_p_id"


def test_buergergeld_einstandspartner_replaces_alg2_from_2023() -> None:
    """From 2023 the Bürgergeld Einstandspartner pointer is in scope, not ALG II's."""
    mapping = get_soep_to_gettsim(datetime.date(2024, 1, 1))
    assert "arbeitslosengeld_2__p_id_einstandspartner" not in mapping
    assert mapping["bürgergeld__p_id_einstandspartner"] == "partner_p_id"


def test_erziehungsgeld_is_in_scope_in_its_pre_2007_window() -> None:
    """The pre-2007 Erziehungsgeld nodes are in scope at an early policy date."""
    assert "erziehungsgeld__budgetsatz" in get_soep_to_gettsim(
        datetime.date(2005, 6, 1)
    )


def test_erziehungsgeld_is_out_of_scope_after_2007() -> None:
    """Erziehungsgeld leaves scope once Elterngeld replaces it from 2007."""
    assert "erziehungsgeld__budgetsatz" not in get_soep_to_gettsim(
        datetime.date(2015, 1, 1)
    )


def test_resolved_mapping_values_match_base() -> None:
    """A date-resolved mapping reuses the date-invariant `BASE_MAPPING` values."""
    mapping = get_soep_to_gettsim(datetime.date(2020, 1, 1))
    assert all(BASE_MAPPING[qname] == value for qname, value in mapping.items())


def test_resolved_qnames_are_a_subset_of_base() -> None:
    """Every resolved qname is one of the candidate qnames in `BASE_MAPPING`."""
    mapping = get_soep_to_gettsim(datetime.date(2020, 1, 1))
    assert set(mapping) <= set(BASE_MAPPING)


def test_date_before_coverage_raises() -> None:
    """A policy date before the covered range raises `ValueError`."""
    with pytest.raises(ValueError, match="No GETTSIM-input mapping period"):
        get_soep_to_gettsim(datetime.date(1900, 1, 1))


def test_pre_2005_period_is_low_confidence() -> None:
    """Periods starting before 2005 are flagged low-confidence (issue #962)."""
    assert period_for_date(datetime.date(2000, 1, 1)).low_confidence is True


def test_recent_period_is_not_low_confidence() -> None:
    """A recent period is not flagged low-confidence."""
    assert period_for_date(datetime.date(2024, 1, 1)).low_confidence is False


def test_periods_are_contiguous_and_non_overlapping() -> None:
    """Consecutive periods join with no gap and no overlap."""
    periods = mapping_periods()
    one_day = datetime.timedelta(days=1)
    assert all(
        later.start_date == earlier.end_date + one_day
        for earlier, later in itertools.pairwise(periods)
    )
