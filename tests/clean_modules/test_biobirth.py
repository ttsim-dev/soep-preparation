"""Tests for the biobirth cleaning module."""

import pandas as pd

from soep_preparation.clean_modules.biobirth import drop_unlisted_children

CHILD_VARIABLES = ["tmp_birth_year_child", "tmp_p_id_child"]
PARENT, CHILDLESS = 10, 20


def _long() -> pd.DataFrame:
    """Two enumeration slots for a mother of one child and a childless person."""
    return pd.DataFrame(
        {
            "hh_id_original": [1, 1, 2, 2],
            "p_id": [PARENT, PARENT, CHILDLESS, CHILDLESS],
            "tmp_child_number": [1, 2, 1, 2],
            "tmp_number_of_children": [1, 1, 0, 0],
            "tmp_birth_year_child": [1980, None, None, None],
            "tmp_p_id_child": [11, None, None, None],
        }
    )


def test_drop_unlisted_children_keeps_one_row_per_listed_child() -> None:
    """A parent keeps the slots holding a child and no others."""
    out = drop_unlisted_children(_long(), child_variables=CHILD_VARIABLES)
    assert out.loc[out["p_id"] == PARENT, "tmp_birth_year_child"].tolist() == [1980]


def test_drop_unlisted_children_keeps_a_person_without_children() -> None:
    """A person who lists no child keeps one row with their number of children."""
    out = drop_unlisted_children(_long(), child_variables=CHILD_VARIABLES)
    assert out.loc[out["p_id"] == CHILDLESS, "tmp_number_of_children"].tolist() == [0]


def test_drop_unlisted_children_marks_the_child_number_missing_without_children() -> (
    None
):
    """The row of a person without a listed child carries no child number."""
    out = drop_unlisted_children(_long(), child_variables=CHILD_VARIABLES)
    assert out.loc[out["p_id"] == CHILDLESS, "tmp_child_number"].isna().all()
