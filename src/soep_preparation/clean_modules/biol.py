"""Clean and convert SOEP biol variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    apply_smallest_int_dtype,
    object_to_bool_categorical,
    object_to_str_categorical,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the biol data file.

    Args:
        raw_data: The raw biol data.

    Returns:
        The processed biol data.
    """
    out = pd.DataFrame()
    out["hh_id"] = apply_smallest_int_dtype(raw_data["hid"])
    out["p_id"] = apply_smallest_int_dtype(raw_data["pid"])
    out["survey_year"] = apply_smallest_int_dtype(raw_data["syear"])

    out["residence_childhood"] = object_to_str_categorical(
        series=raw_data["lb0058"],
        renaming={
            "[4] Auf dem Land": "Auf dem Land",
            "[3] Kleinstadt": "Kleinstadt",
            "[2] Mittelgrosse Stadt": "Mittelgroße Stadt",
            "[1] Grosstadt": "Großstadt",
        },
        ordered=True,
    )
    out["birthplace"] = object_to_str_categorical(raw_data["lb0013_h"])
    out["birthplace_germany_father"] = object_to_bool_categorical(
        series=raw_data["lb0084_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["religion_father"] = object_to_str_categorical(raw_data["lb0124_h"])
    out["birthplace_germany_mother"] = object_to_bool_categorical(
        series=raw_data["lb0085_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["religion_mother"] = object_to_str_categorical(raw_data["lb0125_h"])
    return out
