"""Derive the legal-spouse pointer `ehepartner_p_id` from partner + marital status.

SOEP's `partner_p_id` points to a partner regardless of marital status, so it covers
unmarried cohabiting partners. These tests pin the restriction to a current legal
marriage or registered partnership, as required by GETTSIM's spouse pointer.
"""

import pandas as pd

from soep_preparation.combine_modules.ppathl_pgen import combine


def _ppathl(partner_p_id: int | None) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "p_id": [1],
            "survey_year": [2020],
            "partner_p_id": pd.array([partner_p_id], dtype="int32[pyarrow]"),
        }
    )


def _pgen(marital_status: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "p_id": [1],
            "survey_year": [2020],
            "marital_status": pd.Categorical([marital_status]),
        }
    )


def test_married_person_keeps_partner_as_ehepartner():
    """A married person's spouse pointer is their partner pointer."""
    out = combine(
        ppathl=_ppathl(2), pgen=_pgen("Verheiratet, mit Ehepartner zusammenlebend")
    )
    assert out["ehepartner_p_id"].tolist() == [2]


def test_registered_partnership_keeps_partner_as_ehepartner():
    """A registered partnership counts as a spouse pointer (Ehe for tax since 2013)."""
    out = combine(
        ppathl=_ppathl(2),
        pgen=_pgen("Eingetragene gleichgeschlechtliche Partnerschaft zusammenlebend"),
    )
    assert out["ehepartner_p_id"].tolist() == [2]


def test_unmarried_cohabiting_person_has_no_ehepartner():
    """An unmarried cohabiting partner is not a spouse, so the spouse pointer is NA."""
    out = combine(ppathl=_ppathl(2), pgen=_pgen("Ledig"))
    assert out["ehepartner_p_id"].isna().all()
