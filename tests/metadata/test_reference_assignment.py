"""Test the reference-period assignment for final variables."""

import yaml

from soep_preparation.config import POTENTIAL_INDEX_VARIABLES, SRC
from soep_preparation.create_metadata.reference_assignment import (
    REFERENCE_BY_VARIABLE,
)
from soep_preparation.utilities.reference_period import ReferencePeriod

_CATALOGUE = yaml.safe_load(
    (SRC / "create_metadata" / "variable_to_metadata_mapping.yaml").read_text(
        encoding="utf-8"
    )
)
_NON_INDEX_VARIABLES = sorted(
    name for name in _CATALOGUE if name not in POTENTIAL_INDEX_VARIABLES
)


def test_every_non_index_variable_has_a_reference() -> None:
    """Every non-index catalogue variable has a reference-period assignment."""
    missing = [
        name for name in _NON_INDEX_VARIABLES if name not in REFERENCE_BY_VARIABLE
    ]
    assert missing == []


def test_no_extra_variables_in_assignment() -> None:
    """The assignment maps no variable absent from the catalogue."""
    extra = sorted(set(REFERENCE_BY_VARIABLE) - set(_NON_INDEX_VARIABLES))
    assert extra == []


def test_no_index_variable_is_assigned() -> None:
    """Index variables are excluded from the reference-period assignment."""
    assigned_index = [
        name for name in POTENTIAL_INDEX_VARIABLES if name in REFERENCE_BY_VARIABLE
    ]
    assert assigned_index == []


def test_all_assigned_values_are_valid_reference_periods() -> None:
    """Every assigned value is a `ReferencePeriod` member."""
    invalid = sorted(
        name
        for name, reference in REFERENCE_BY_VARIABLE.items()
        if not isinstance(reference, ReferencePeriod)
    )
    assert invalid == []
