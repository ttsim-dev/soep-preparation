"""Functions to pre-process variables for a raw kidlong dataset.

Functions:
- clean: Coordinates the pre-processing for the dataset.

Usage:
    Import this module and call clean to pre-process variables.
"""

import pandas as pd

from soep_preparation.utilities import (
    float_categorical_to_float,
    int_categorical_to_int,
)


def clean(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the kidlong dataset."""
    out = pd.DataFrame()
    out["hh_id"] = int_categorical_to_int(raw["hid"])
    out["p_id"] = int_categorical_to_int(raw["pid"])
    out["survey_year"] = int_categorical_to_int(raw["syear"])
    out["pointer_mother"] = int_categorical_to_int(raw["k_pmum"])
    out["betreuungskost_einrichtung"] = float_categorical_to_float(
        raw["kk_amtp_h"],
    )
    out["school_costs"] = float_categorical_to_float(raw["ks_amtp_h"])

    return out
