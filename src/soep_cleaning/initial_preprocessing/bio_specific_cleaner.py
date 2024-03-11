import pandas as pd

from soep_cleaning.config import MONTH_MAPPING
from soep_cleaning.initial_preprocessing.helper import (
    biobirth_wide_to_long,
    bool_categorical,
    int_categorical_to_int,
    str_categorical,
)
from soep_cleaning.utilities import apply_lowest_int_dtype


def bioedu(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the bioedu dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = apply_lowest_int_dtype(raw_data["cid"])
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])

    out["birth_month"] = str_categorical(
        raw_data["gebmonat"],
        ordered=False,
        renaming=MONTH_MAPPING,
    )
    return out


def biobirth(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biobirth dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = int_categorical_to_int(raw_data["cid"])
    out["p_id"] = int_categorical_to_int(raw_data["pid"])

    out["n_kids_total"] = int_categorical_to_int(raw_data["sumkids"])

    for i in range(1, 16):
        two_digit = f"{i:02d}"
        out[f"birth_year_child_{i}"] = int_categorical_to_int(
            raw_data[f"kidgeb{two_digit}"],
        )
        out[f"p_id_child_{i}"] = int_categorical_to_int(
            raw_data[f"kidpnr{two_digit}"],
        )
        out[f"birth_month_child_{i}"] = str_categorical(
            raw_data[f"kidmon{two_digit}"],
            ordered=False,
        )

    return biobirth_wide_to_long(out)


def biol(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["soep_hh_id"] = int_categorical_to_int(raw_data["hid"])
    out["p_id"] = int_categorical_to_int(raw_data["pid"])

    out["year"] = int_categorical_to_int(raw_data["syear"])

    out["birthplace"] = str_categorical(
        raw_data["lb0013_h"],
        ordered=False,
    )
    out["res_childhood"] = str_categorical(
        raw_data["lb0058"],
        ordered=False,
    )
    out["birthplace_father"] = bool_categorical(
        raw_data["lb0084_h"],
        renaming={"Ja": True, "Nein": False},
    )
    out["birthplace_mother"] = bool_categorical(
        raw_data["lb0085_h"],
        renaming={"Ja": True, "Nein": False},
    )
    out["religion_father"] = str_categorical(
        raw_data["lb0124_h"],
        ordered=False,
    )
    out["religion_mother"] = str_categorical(
        raw_data["lb0125_h"],
        ordered=False,
    )

    return out
