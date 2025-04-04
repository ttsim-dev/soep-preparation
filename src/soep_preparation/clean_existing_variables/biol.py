"""Functions to pre-process variables for a raw biol dataset.

Functions:
- clean: Coordinates the pre-processing for the dataset.

Usage:
    Import this module and call clean to pre-process variables.
"""

import pandas as pd

from soep_preparation.utilities import (
    apply_lowest_int_dtype,
    bool_categorical,
    int_categorical_to_int,
    str_categorical,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["hh_id"] = int_categorical_to_int(raw_data["hid"])
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])

    out["survey_year"] = int_categorical_to_int(raw_data["syear"])

    out["birthplace"] = str_categorical(raw_data["lb0013_h"])
    out["res_childhood"] = str_categorical(raw_data["lb0058"])
    out["birthplace_father"] = bool_categorical(
        raw_data["lb0084_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["birthplace_mother"] = bool_categorical(
        raw_data["lb0085_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["religion_father"] = str_categorical(raw_data["lb0124_h"])
    out["religion_mother"] = str_categorical(raw_data["lb0125_h"])
    return out
