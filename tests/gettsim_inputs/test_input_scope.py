"""Tests for the committed per-policy-date GETTSIM input scope.

The freshness test regenerates the scope from GETTSIM and fails if the committed file
has drifted; it is skipped where GETTSIM is not importable (the default soep-preparation
environment), mirroring the metadata gate. The invariant test needs no GETTSIM.
"""

import json

import pytest

from soep_preparation.gettsim_inputs import _generate_input_scope
from soep_preparation.gettsim_inputs.mapping import BASE_MAPPING, mapping_periods


def test_every_in_scope_qname_is_a_base_qname() -> None:
    """No period lists an in-scope qname that is absent from `BASE_MAPPING`."""
    base_qnames = set(BASE_MAPPING)
    assert all(period.in_scope <= base_qnames for period in mapping_periods())


def test_committed_scope_matches_regenerated_scope() -> None:
    """The committed scope file equals what the generator produces from GETTSIM."""
    pytest.importorskip("gettsim")
    regenerated = _generate_input_scope.build_periods()
    committed = json.loads(
        _generate_input_scope.SCOPE_PATH.read_text(encoding="utf-8")
    )["periods"]
    assert regenerated == committed, (
        "gettsim_input_scope.json is stale; regenerate it with "
        "_generate_input_scope.py."
    )
