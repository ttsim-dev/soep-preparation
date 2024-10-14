from soep_preparation.config import pd
from soep_preparation.utilities import (
    apply_lowest_int_dtype,
    bool_categorical,
    int_categorical_to_int,
    str_categorical,
)


def clean(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["hh_id"] = int_categorical_to_int(raw["hid"])
    out["p_id"] = apply_lowest_int_dtype(raw["pid"])

    out["year"] = int_categorical_to_int(raw["syear"])

    out["birthplace"] = str_categorical(raw["lb0013_h"])
    out["res_childhood"] = str_categorical(raw["lb0058"])
    out["birthplace_father"] = bool_categorical(
        raw["lb0084_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["birthplace_mother"] = bool_categorical(
        raw["lb0085_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["religion_father"] = str_categorical(raw["lb0124_h"])
    out["religion_mother"] = str_categorical(raw["lb0125_h"])
    return out
