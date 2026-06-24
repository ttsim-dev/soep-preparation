"""Tests for the GETTSIM-input mapping.

The mapping must be honest: every non-`None` target must name a real SOEP final
variable, and the keys must be GETTSIM input qnames (`__`-qualified) rather than SOEP
names.
"""

import pytest

from soep_preparation.config import METADATA, POTENTIAL_INDEX_VARIABLES
from soep_preparation.gettsim_inputs.mapping import SOEP_TO_GETTSIM

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


def test_pension_year_maps_to_first_receipt_year() -> None:
    """`sozialversicherung__rente__jahr_renteneintritt` uses first pension receipt."""
    assert (
        SOEP_TO_GETTSIM["sozialversicherung__rente__jahr_renteneintritt"]
        == "first_pension_receipt_year"
    )


def test_steuerklasse_has_no_soep_source() -> None:
    """`lohnsteuer__steuerklasse` has no SOEP source and stays unmapped."""
    assert SOEP_TO_GETTSIM["lohnsteuer__steuerklasse"] is None


def test_all_keys_are_gettsim_qnames_not_soep_names() -> None:
    """GETTSIM-input keys must not collide with SOEP final variable names."""
    soep_named_keys = set(SOEP_TO_GETTSIM) & set(METADATA)
    assert soep_named_keys == set()
