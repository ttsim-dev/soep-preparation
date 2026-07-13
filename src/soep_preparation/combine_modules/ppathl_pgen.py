"""Derive the legal-spouse pointer `ehepartner_p_id` from ppathl + pgen.

SOEP's `partner_p_id` (ppathl) points to a person's partner regardless of marital
status, so it covers unmarried cohabiting partners as well as spouses. GETTSIM's
`familie__p_id_ehepartner` is specifically the spouse pointer (it drives joint
taxation), so this restricts `partner_p_id` to people whose `marital_status` (pgen)
denotes a current legal marriage or registered partnership.
"""

import pandas as pd

from soep_preparation.utilities.data_manipulator import apply_smallest_int_dtype

# `marital_status` (SOEP `pgfamstd`) values denoting a current legal spouse or
# registered partnership. Registered same-sex partnerships count as marriage for tax
# since 2013; the separated and abroad statuses keep the legal tie (their
# `partner_p_id` is usually NA anyway). The exact set is a maintainer-reviewable choice.
_SPOUSE_STATUSES = frozenset(
    {
        "Verheiratet, mit Ehepartner zusammenlebend",
        "Verheiratet, dauernd getrennt lebend",
        "Ehepartner im Ausland",
        "Eingetragene gleichgeschlechtliche Partnerschaft zusammenlebend",
        "Eingetragene gleichgeschlechtliche Partnerschaft getrennt lebend",
    }
)


def combine(ppathl: pd.DataFrame, pgen: pd.DataFrame) -> pd.DataFrame:
    """Derive `ehepartner_p_id` only.

    `ehepartner_p_id` equals `partner_p_id` for people whose `marital_status` denotes a
    current legal spouse or registered partnership, and is `pd.NA` otherwise.

    Args:
        ppathl: Cleaned ppathl module (carries `partner_p_id`).
        pgen: Cleaned pgen module (carries `marital_status`).

    Returns:
        Person-year frame carrying `ehepartner_p_id`.
    """
    merged = pd.merge(
        ppathl[["p_id", "survey_year", "partner_p_id"]],
        pgen[["p_id", "survey_year", "marital_status"]],
        on=["p_id", "survey_year"],
        how="outer",
    )
    is_spouse = merged["marital_status"].isin(_SPOUSE_STATUSES)

    out = pd.DataFrame(index=merged.index)
    out["p_id"] = merged["p_id"]
    out["survey_year"] = merged["survey_year"]
    out["ehepartner_p_id"] = apply_smallest_int_dtype(
        merged["partner_p_id"].where(is_spouse, other=pd.NA)
    )
    return out
