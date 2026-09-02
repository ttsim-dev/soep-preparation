"""Test carrying metadata forward for modules that did not run."""

from soep_preparation.create_metadata.task import _carry_forward_skipped_modules

_EXISTING_CIRDEF_ENTRY = {
    "module": "cirdef",
    "dtype": "bool[pyarrow]",
    "survey_years": None,
    "reference": "no_reference",
}


def test_carry_forward_keeps_entries_of_skipped_module_with_source_file() -> None:
    """A module whose raw data file is absent keeps its metadata entries."""
    result = _carry_forward_skipped_modules(
        new_mapping={},
        existing_mapping={"teaching_sample": _EXISTING_CIRDEF_ENTRY},
        present_modules=set(),
    )

    assert result == {"teaching_sample": _EXISTING_CIRDEF_ENTRY}


def test_carry_forward_drops_entries_of_module_without_source_file() -> None:
    """A deleted module's entries stay out, so its removal is still reported."""
    result = _carry_forward_skipped_modules(
        new_mapping={},
        existing_mapping={
            "some_variable": {**_EXISTING_CIRDEF_ENTRY, "module": "deleted_module"}
        },
        present_modules=set(),
    )

    assert result == {}


def test_carry_forward_prefers_new_metadata_for_processed_module() -> None:
    """A module that ran defines its own metadata; the existing entry is ignored."""
    new_entry = {**_EXISTING_CIRDEF_ENTRY, "dtype": "int8[pyarrow]"}

    result = _carry_forward_skipped_modules(
        new_mapping={"rgroup20": new_entry},
        existing_mapping={"rgroup20": _EXISTING_CIRDEF_ENTRY},
        present_modules={"cirdef"},
    )

    assert result == {"rgroup20": new_entry}
