"""Infer Erwerbsminderungsrente receipt from own-pension receipt and age."""

import pandas as pd

# Earliest statutory old-age pension entry age (vorzeitige Altersrente für
# langjährig Versicherte). Own-pension receipt below this age cannot be an
# old-age pension, so it is taken as Erwerbsminderungsrente. rmsm-data may
# sharpen this with the cohort-specific Altersgrenze.
_EARLIEST_OLD_AGE_CLAIMING_AGE = 63


def combine(pl: pd.DataFrame, pbrutto: pd.DataFrame) -> pd.DataFrame:
    """Infer EM-Rente receipt: own pension drawn below the earliest old-age age.

    The questionnaire does not separate Altersrente from Erwerbsminderungsrente,
    so receipt of an own pension before the earliest old-age claiming age is used
    as the EM-Rente indicator. The final `work_disabled` mapping (e.g. combining
    this with `recognized_reduced_earning_capacity`) is calibrated in rmsm-data.

    Args:
        pl: Cleaned pl module (own-pension receipt, reduced earning capacity).
        pbrutto: Cleaned pbrutto module (birth year).

    Returns:
        Person-year frame with the inferred EM-Rente indicator.
    """
    merged = pd.merge(
        pl[
            [
                "p_id",
                "hh_id",
                "survey_year",
                "bezieht_eigene_rente",
                "recognized_reduced_earning_capacity",
            ]
        ],
        pbrutto[["p_id", "survey_year", "birth_year"]],
        on=["p_id", "survey_year"],
        how="inner",
    )
    out = pd.DataFrame(index=merged.index)
    out["p_id"] = merged["p_id"]
    out["hh_id"] = merged["hh_id"]
    out["survey_year"] = merged["survey_year"]

    age = merged["survey_year"] - merged["birth_year"]
    receives_own_pension = (
        merged["bezieht_eigene_rente"].astype("boolean").fillna(value=False)
    )
    below_old_age = (age < _EARLIEST_OLD_AGE_CLAIMING_AGE).fillna(value=False)
    out["erwerbsminderungsrente_inferred"] = (
        receives_own_pension & below_old_age
    ).astype("boolean")
    return out
