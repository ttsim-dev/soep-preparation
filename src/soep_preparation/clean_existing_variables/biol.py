"""Functions to pre-process variables for a raw biol dataset."""

import pandas as pd

from soep_preparation.utilities import (
    apply_lowest_int_dtype,
    object_to_bool_categorical,
    object_to_str_categorical,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])

    out["survey_year"] = apply_lowest_int_dtype(raw_data["syear"])

    out["birthplace"] = object_to_str_categorical(raw_data["lb0013_h"])
    out["res_childhood"] = object_to_str_categorical(raw_data["lb0058"])
    out["birthplace_father"] = object_to_bool_categorical(
        raw_data["lb0084_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["birthplace_mother"] = object_to_bool_categorical(
        raw_data["lb0085_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["religion_father"] = object_to_str_categorical(raw_data["lb0124_h"])
    out["religion_mother"] = object_to_str_categorical(raw_data["lb0125_h"])
    return out
