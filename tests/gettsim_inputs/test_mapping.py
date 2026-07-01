"""Tests for the GETTSIM-input mapping.

The mapping must be honest: every non-`None` target must name a real SOEP final
variable, and the keys must be GETTSIM input qnames (`__`-qualified) rather than SOEP
names.
"""

import pytest

from soep_preparation.config import METADATA, POTENTIAL_INDEX_VARIABLES
from soep_preparation.gettsim_inputs.mapping import GAP_NOTES, SOEP_TO_GETTSIM

_VALID_SOEP_TARGETS = set(METADATA) | set(POTENTIAL_INDEX_VARIABLES)

_NON_NONE_ITEMS = [
    (gettsim_qname, soep_variable)
    for gettsim_qname, soep_variable in SOEP_TO_GETTSIM.items()
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
    assert SOEP_TO_GETTSIM["wohnen__wohnfläche_hh"] == "living_space_hh"


def test_pension_entry_year_is_unmapped() -> None:
    """The statutory pension-entry year stays unmapped: SOEP has no claim-start event.

    `first_pension_receipt_year` is the first year with positive statutory-pension
    income (or a retirement-calendar status proxy), which is left-censored by late
    panel entry and includes non-statutory states — not the actual
    `jahr_renteneintritt`. Mapping it would change pension calculations.
    """
    assert SOEP_TO_GETTSIM["sozialversicherung__rente__jahr_renteneintritt"] is None


def test_pension_entry_year_gap_is_documented() -> None:
    """The unmapped pension-entry year is recorded in `GAP_NOTES`."""
    assert "sozialversicherung__rente__jahr_renteneintritt" in GAP_NOTES


def test_maintenance_received_is_unmapped() -> None:
    """Generic maintenance received stays unmapped: SOEP only has child maintenance.

    `unterhalt__tatsächlich_erhaltener_betrag_m` is generic maintenance actually
    received (any kind), whereas the SOEP `kindesunterhalt_received_m` is child
    maintenance specifically. Feeding the narrower child-only amount into the generic
    input would understate maintenance, so it stays unmapped.
    """
    assert SOEP_TO_GETTSIM["unterhalt__tatsächlich_erhaltener_betrag_m"] is None


def test_maintenance_received_gap_is_documented() -> None:
    """The unmapped generic maintenance-received input is recorded in `GAP_NOTES`."""
    assert "unterhalt__tatsächlich_erhaltener_betrag_m" in GAP_NOTES


def test_steuerklasse_has_no_soep_source() -> None:
    """`lohnsteuer__steuerklasse` has no SOEP source and stays unmapped."""
    assert SOEP_TO_GETTSIM["lohnsteuer__steuerklasse"] is None


def test_ehepartner_maps_to_marriage_restricted_pointer() -> None:
    """The spouse pointer uses `ehepartner_p_id`, not the generic partner pointer."""
    assert SOEP_TO_GETTSIM["familie__p_id_ehepartner"] == "ehepartner_p_id"


@pytest.mark.parametrize(
    "gettsim_qname",
    [
        "bürgergeld__p_id_einstandspartner",
        "arbeitslosengeld_2__p_id_einstandspartner",
    ],
)
def test_einstandspartner_maps_to_generic_partner_pointer(gettsim_qname: str) -> None:
    """The Einstandspartner uses the generic `partner_p_id`, including unmarried."""
    assert SOEP_TO_GETTSIM[gettsim_qname] == "partner_p_id"


def test_all_keys_are_gettsim_qnames_not_soep_names() -> None:
    """GETTSIM-input keys must not collide with SOEP final variable names."""
    soep_named_keys = set(SOEP_TO_GETTSIM) & set(METADATA)
    assert soep_named_keys == set()
